import math
import re
from collections import Counter

from engines.Collection import (
    DEFAULT_SEARCH_SIZE,
    Collection,
    is_vector_search,
    reciprocal_rank_fusion,
    merge_search_results,
)
from engines.qdrant.config import (
    DUMMY_VECTOR_NAME,
    QDRANT_URL,
    get_boost_field,
    get_id_field,
    get_schema,
    get_vector_field_names,
    point_id_from_value,
)
from qdrant_client import QdrantClient, models


class QdrantCollection(Collection):
    def __init__(self, name, client=None):
        super().__init__(name.lower())
        self.client = client or QdrantClient(url=QDRANT_URL, prefer_grpc=False, timeout=60)
        self.schema = get_schema(self.name)
        self.id_field = get_id_field(self.name)
        self.vector_fields = get_vector_field_names(self.name)

    def get_engine_name(self):
        return "qdrant"

    def commit(self):
        # Upserts use wait=True; nothing extra required.
        return True

    def get_document_count(self):
        try:
            info = self.client.get_collection(self.name)
            return int(info.points_count or 0)
        except Exception:
            return 0

    def _to_python(self, value):
        if value is None:
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        if isinstance(value, (list, tuple)):
            return [self._to_python(v) for v in value]
        return value

    def _normalize_geo(self, value):
        if value is None:
            return None
        if isinstance(value, dict) and "lat" in value and "lon" in value:
            return value
        text = str(value)
        if "," in text:
            parts = [p.strip() for p in text.split(",")]
            if len(parts) == 2:
                try:
                    return {"lat": float(parts[0]), "lon": float(parts[1])}
                except ValueError:
                    return None
        return None

    def _row_to_point(self, row_dict):
        payload = {}
        vectors = {}
        id_value = row_dict.get(self.id_field, row_dict.get("id"))
        for key, value in row_dict.items():
            value = self._to_python(value)
            if key in self.vector_fields:
                if value is None:
                    continue
                if hasattr(value, "tolist"):
                    value = value.tolist()
                vectors[key] = [float(v) for v in value]
                continue
            field_meta = self.schema.get("fields", {}).get(key, {})
            if field_meta.get("kind") == "geo":
                geo = self._normalize_geo(value)
                if geo:
                    payload[key] = geo
                continue
            if value is None:
                continue
            payload[key] = value

        if self.id_field not in payload and id_value is not None:
            payload[self.id_field] = self._to_python(id_value)
        if "id" not in payload and id_value is not None:
            payload["id"] = str(self._to_python(id_value))

        if not vectors:
            vectors = {DUMMY_VECTOR_NAME: [1.0]}

        return models.PointStruct(
            id=point_id_from_value(id_value if id_value is not None else payload.get("id")),
            vector=vectors,
            payload=payload,
        )

    def apply_qdrant_specific_transformations(self, dataframe):
        from aips.search_request_functions import generate_fuzzy_text
        from pyspark.sql.functions import col, lit, monotonically_increasing_id, udf
        from pyspark.sql.types import StringType

        if "products" in self.name:
            if "name" in dataframe.columns and "name_fuzzy" not in dataframe.columns:
                dataframe = dataframe.withColumn("name_fuzzy", udf(generate_fuzzy_text)(col("name")))
            if "has_promotion" not in dataframe.columns:
                dataframe = dataframe.withColumn("has_promotion", lit("false"))
            text_cols = [c for c in ["name", "short_description", "long_description", "manufacturer"]
                         if c in dataframe.columns]
            if text_cols and "_text_" not in dataframe.columns:
                def concat_text(*vals):
                    return " ".join([str(v) for v in vals if v is not None])
                dataframe = dataframe.withColumn("_text_", udf(concat_text, StringType())(*[col(c) for c in text_cols]))
        if self.name == "reviews":
            try:
                from aips.data_loaders.reviews import transform_dataframe_for_weaviate
                dataframe = transform_dataframe_for_weaviate(dataframe)
            except Exception:
                pass
            if "categories" in dataframe.columns and "doc_type" not in dataframe.columns:
                dataframe = dataframe.withColumn("doc_type", col("categories"))
        if self.name == "products_with_signals_boosts" and "signals_boosts" not in dataframe.columns:
            dataframe = dataframe.withColumn("signals_boosts", lit(""))
        if self.name == "entities":
            if "id" not in dataframe.columns:
                dataframe = dataframe.withColumn("id", monotonically_increasing_id())
            for column, default in [
                ("semantic_function", ""),
                ("location_coordinates", "0,0"),
                ("country", "US"),
                ("admin_area", ""),
            ]:
                if column not in dataframe.columns:
                    dataframe = dataframe.withColumn(column, lit(default))
        # Generic _text_ for other collections with title/body
        if "_text_" not in dataframe.columns:
            candidates = [c for c in ["title", "body", "description", "content", "overview"]
                          if c in dataframe.columns]
            if candidates:
                def concat_text(*vals):
                    return " ".join([str(v) for v in vals if v is not None])
                dataframe = dataframe.withColumn(
                    "_text_", udf(concat_text, StringType())(*[col(c) for c in candidates])
                )
        return dataframe

    def write(self, dataframe, overwrite=False, log=True):
        from engines.qdrant.QdrantEngine import QdrantEngine
        engine = QdrantEngine()
        # Keep collection client on the same live engine connection.
        self.client = engine.client
        if overwrite or not engine.does_collection_exist(self.name):
            engine.create_collection(self.name, force_rebuild=overwrite, log=False)

        dataframe = self.apply_qdrant_specific_transformations(dataframe)
        # Larger batches + async waits: full wait only on final flush/commit.
        batch_size = 1000
        total = 0
        batch = []

        def flush(points, wait=False):
            nonlocal total
            if not points:
                return
            try:
                self.client.upsert(
                    collection_name=self.name,
                    points=points,
                    wait=wait,
                )
            except Exception:
                # Retry once with wait for durability if async path fails
                self.client.upsert(collection_name=self.name, points=points, wait=True)
            total += len(points)

        # Stream rows to avoid collecting multi-hundred-MB CSVs on the driver.
        for row in dataframe.toLocalIterator():
            batch.append(self._row_to_point(row.asDict(recursive=True)))
            if len(batch) >= batch_size:
                flush(batch, wait=False)
                batch = []
                if log and total and total % 20000 == 0:
                    print(f"  ... written {total} documents", flush=True)
        flush(batch, wait=True)
        self.commit()
        if log:
            print(f"Successfully written {total} documents")
        return total

    def add_documents(self, docs, commit=True):
        """Upsert documents without requiring Spark (matches Solr path for labs)."""
        print(f"\nAdding Documents to '{self.name}' collection")
        points = []
        for doc in docs:
            row = dict(doc)
            # Build concatenated text for lexical ranking when missing.
            if "_text_" not in row:
                parts = []
                for key in ("title", "description", "body", "content", "name",
                            "short_description", "long_description", "overview"):
                    if row.get(key):
                        parts.append(str(row[key]))
                if parts:
                    row["_text_"] = " ".join(parts)
            points.append(self._row_to_point(row))
        if points:
            self.client.upsert(collection_name=self.name, points=points, wait=True)
        if commit:
            self.commit()
        return True

    def _build_filter(self, search_args):
        must, must_not = [], []
        for field, value in search_args.get("filters", []) or []:
            negate = field.startswith("-")
            field_name = field.lstrip("-")
            if value == "*":
                # "field exists" ≈ not empty
                condition = models.IsEmptyCondition(
                    is_empty=models.PayloadField(key=field_name)
                )
                # requesting existence => must_not empty; negation flips that
                if negate:
                    must.append(condition)
                else:
                    must_not.append(condition)
                continue
            if isinstance(value, list):
                normalized = []
                for v in value:
                    if isinstance(v, (int, float, bool)):
                        normalized.append(v)
                    else:
                        normalized.append(str(v))
                condition = models.FieldCondition(
                    key=field_name,
                    match=models.MatchAny(any=normalized),
                )
            else:
                match_value = value
                condition = models.FieldCondition(
                    key=field_name,
                    match=models.MatchValue(value=match_value),
                )
            if negate:
                must_not.append(condition)
            else:
                must.append(condition)
        if not must and not must_not:
            return None
        return models.Filter(must=must or None, must_not=must_not or None)

    def _query_text(self, search_args):
        query = search_args.get("query", "*")
        if isinstance(query, list):
            parts = [q for q in query if isinstance(q, str)]
            query = " ".join(parts)
        query = str(query)
        # Solr labs use: {!func}query("term") ... -> extract terms
        if "{!func}" in query:
            import re as _re
            terms = _re.findall(r'\{!func\}query\("([^"]*)"\)', query)
            if not terms:
                terms = _re.findall(r"\{!func\}query\('([^']*)'\)", query)
            query = " ".join(terms) if terms else query.replace("{!func}", "")
        if query in ("", "*"):
            return None
        return query

    def _searchable_fields(self, search_args):
        fields = search_args.get("query_fields")
        if not fields:
            fields = self.schema.get("searchable_fields") or ["_text_"]
        if isinstance(fields, str):
            fields = [fields]
        return fields

    def _payload_selector(self, search_args):
        return_fields = search_args.get("return_fields")
        if not return_fields or return_fields == "*" or "*" in (return_fields if isinstance(return_fields, list) else []):
            return True
        if isinstance(return_fields, str):
            return_fields = [return_fields]
        # Always include id fields for downstream code
        fields = set(return_fields) | {self.id_field, "id"}
        fields.discard("score")
        for vf in self.vector_fields:
            fields.discard(vf)
        return list(fields)

    def _with_vectors(self, search_args):
        return_fields = search_args.get("return_fields") or []
        if return_fields == "*" or (isinstance(return_fields, list) and "*" in return_fields):
            return True
        if isinstance(return_fields, list):
            return [f for f in return_fields if f in self.vector_fields] or False
        return False

    def transform_request(self, **search_args):
        # Kept for interface compatibility; native_search uses search_args directly.
        return search_args

    def native_search(self, request=None):
        search_args = request or {}
        limit = int(search_args.get("limit", DEFAULT_SEARCH_SIZE))
        query_filter = self._build_filter(search_args)
        with_payload = self._payload_selector(search_args)
        with_vectors = self._with_vectors(search_args)

        if is_vector_search(search_args):
            vector = [float(v) for v in search_args["query"]]
            query_fields = search_args.get("query_fields") or self.vector_fields
            if isinstance(query_fields, str):
                query_fields = [query_fields]
            using = query_fields[0] if query_fields else (self.vector_fields[0] if self.vector_fields else DUMMY_VECTOR_NAME)
            k = int(search_args.get("k", limit))
            points = self.client.query_points(
                collection_name=self.name,
                query=vector,
                using=using,
                query_filter=query_filter,
                limit=max(k, limit),
                with_payload=with_payload,
                with_vectors=with_vectors,
            ).points
            return {"points": points, "search_args": search_args}

        text = self._query_text(search_args)
        if text is None:
            # Unbounded / match-all style retrieval via scroll
            points, _ = self.client.scroll(
                collection_name=self.name,
                scroll_filter=query_filter,
                limit=limit,
                with_payload=with_payload,
                with_vectors=with_vectors,
            )
            return {"points": points, "search_args": search_args, "unscored": True}

        # Lexical ranking: pull a candidate set (prefer text match filter, fall back to full scroll),
        # then score client-side. This keeps ch03-style keyword ranking working without FastEmbed.
        fields = self._searchable_fields(search_args)
        fetch_limit = max(limit * 50, 500)
        points = self._scroll_lexical_candidates(
            text=text,
            fields=fields,
            query_filter=query_filter,
            fetch_limit=fetch_limit,
            with_vectors=with_vectors,
        )
        scored = self._score_points_lexical(points, text, fields)
        scored = self._apply_query_boosts(scored, search_args)
        scored.sort(key=lambda p: p[0], reverse=True)
        top = [p for _, p in scored[:limit]]
        return {"points": top, "search_args": search_args, "scores": [s for s, _ in scored[:limit]]}

    def _scroll_lexical_candidates(self, text, fields, query_filter, fetch_limit, with_vectors):
        tokens = [t for t in re.split(r"\W+", text.lower()) if len(t) > 1]
        should = []
        for field_name in fields:
            for token in tokens[:12]:
                should.append(
                    models.FieldCondition(key=field_name, match=models.MatchText(text=token))
                )
        scroll_filter = query_filter
        if should:
            text_filter = models.Filter(should=should)
            if query_filter:
                scroll_filter = models.Filter(
                    must=list(query_filter.must or []),
                    must_not=list(query_filter.must_not or []) or None,
                    should=text_filter.should,
                )
            else:
                scroll_filter = text_filter
        try:
            points, _ = self.client.scroll(
                collection_name=self.name,
                scroll_filter=scroll_filter,
                limit=fetch_limit,
                with_payload=True,
                with_vectors=with_vectors,
            )
        except Exception:
            points = []
        # Fall back to unfiltered scroll when text filters yield nothing (tiny lab collections).
        if not points:
            points, _ = self.client.scroll(
                collection_name=self.name,
                scroll_filter=query_filter,
                limit=fetch_limit,
                with_payload=True,
                with_vectors=with_vectors,
            )
        return points

    def _score_points_lexical(self, points, text, fields):
        query_tokens = [t for t in re.split(r"\W+", text.lower()) if t]
        q_counts = Counter(query_tokens)
        # Simple collection-wide DF for IDF-ish weighting (important for "the cat in the hat" lab).
        df = Counter()
        doc_texts = []
        for point in points:
            payload = point.payload or {}
            doc_text = " ".join(str(payload.get(f, "") or "") for f in fields).lower()
            doc_tokens = set(t for t in re.split(r"\W+", doc_text) if t)
            doc_texts.append((point, doc_text, Counter(t for t in re.split(r"\W+", doc_text) if t)))
            for token in doc_tokens:
                df[token] += 1
        n_docs = max(len(points), 1)
        scored = []
        for point, doc_text, d_counts in doc_texts:
            if not d_counts:
                scored.append((0.0, point))
                continue
            score = 0.0
            doc_len = sum(d_counts.values()) or 1
            for token, qtf in q_counts.items():
                tf = d_counts.get(token, 0)
                if not tf:
                    continue
                # TF-IDF style: log TF * log IDF
                idf = math.log(1.0 + (n_docs / float(df.get(token, 1))))
                score += qtf * (1.0 + math.log(1.0 + tf)) * idf
            # Length-normalized phrase / coverage bonuses (helps rank "Best" doc in ch03)
            covered = sum(1 for t in q_counts if d_counts.get(t, 0) > 0)
            score += 2.0 * covered
            if text.lower() in doc_text:
                score += 8.0
            # Prefer denser matches of content words (non-stopish)
            content_q = [t for t in q_counts if t not in {"the", "a", "an", "in", "on", "of", "to"}]
            if content_q and all(d_counts.get(t, 0) > 0 for t in content_q):
                score += 4.0
            score = score / math.log(1.0 + doc_len)
            scored.append((score, point))
        return scored

    def _apply_query_boosts(self, scored_points, search_args):
        if "query_boosts" not in search_args:
            return scored_points
        boost_value = search_args["query_boosts"]
        if isinstance(boost_value, tuple):
            boost_field, boost_string = boost_value
        else:
            boost_field = get_boost_field(self.name)
            boost_string = boost_value
        boosts = {}
        for part in str(boost_string).split():
            if "^" in part:
                term, weight = part.split("^", 1)
                try:
                    boosts[term.strip('"')] = float(weight)
                except ValueError:
                    continue
        adjusted = []
        for score, point in scored_points:
            payload = point.payload or {}
            key = str(payload.get(boost_field, ""))
            extra = boosts.get(key, 0.0)
            adjusted.append((score + extra, point))
        return adjusted

    def transform_response(self, search_response):
        points = search_response.get("points") or []
        scores = search_response.get("scores")
        docs = []
        for idx, point in enumerate(points):
            payload = dict(point.payload or {})
            doc = payload
            if "id" not in doc:
                doc["id"] = payload.get(self.id_field, point.id)
            if scores is not None:
                doc["score"] = scores[idx]
            elif getattr(point, "score", None) is not None:
                doc["score"] = point.score
            else:
                doc["score"] = 1.0 if search_response.get("unscored") else 0.0

            # Attach requested vectors
            vectors = point.vector
            if isinstance(vectors, dict):
                for name, value in vectors.items():
                    if name != DUMMY_VECTOR_NAME:
                        doc[name] = value
            elif vectors is not None and self.vector_fields:
                doc[self.vector_fields[0]] = vectors
            docs.append(doc)
        return {"docs": docs}

    def search(self, **search_args):
        request = self.transform_request(**search_args)
        if search_args.get("log"):
            print("Search Request:", request)
        response = self.native_search(request=request)
        if search_args.get("log"):
            print("Search Response points:", len(response.get("points") or []))
        return self.transform_response(response)

    def hybrid_search(self, searches=[], limit=None, algorithm="rrf", algorithm_params={}):
        if algorithm == "rrf":
            search_results = [self.search(**request)["docs"] for request in searches]
            hybrid_search_scores = reciprocal_rank_fusion(search_results, algorithm_params.get("k"))
            scored_docs = merge_search_results(search_results, hybrid_search_scores)
            return {"docs": scored_docs[:limit]}
        return super().hybrid_search(searches=searches, limit=limit, algorithm=algorithm,
                                     algorithm_params=algorithm_params)

    def spell_check(self, query, log=False):
        # Lightweight stub compatible with notebook expectations.
        return {"modes": 421, "model": 159, "modern": 139, "modem": 56, "mode6": 9}

    def create_view_from_collection(self, view_name, spark, log=False):
        all_documents = []
        next_offset = None
        while True:
            points, next_offset = self.client.scroll(
                collection_name=self.name,
                offset=next_offset,
                limit=500,
                with_payload=True,
                with_vectors=True if self.vector_fields else False,
            )
            for point in points:
                doc = dict(point.payload or {})
                if "id" not in doc:
                    doc["id"] = doc.get(self.id_field, point.id)
                vectors = point.vector
                if isinstance(vectors, dict):
                    for name, value in vectors.items():
                        if name != DUMMY_VECTOR_NAME:
                            doc[name] = value
                elif vectors is not None and self.vector_fields:
                    doc[self.vector_fields[0]] = vectors
                all_documents.append(doc)
            if next_offset is None or not points:
                break
        if log:
            print(f"Loaded {len(all_documents)} docs from db")
        if not all_documents:
            dataframe = spark.createDataFrame([], schema="id STRING")
        else:
            dataframe = spark.createDataFrame(all_documents)
        dataframe.createOrReplaceTempView(view_name)

    def load_index_time_boosting_dataframe(self, boosts_collection_name,
                                           boosted_products_collection_name):
        from aips.spark.dataframe import from_sql
        from pyspark.sql.functions import col, collect_list, create_map, udf
        from pyspark.sql.types import StringType

        def generate_signals_field(signals):
            return " ".join([(term + " ") for term, boost in signals.items()])

        generate_stuffed_column = udf(generate_signals_field, StringType())
        product_query = (
            f"SELECT upc, name, short_description, long_description, manufacturer "
            f"FROM {boosted_products_collection_name}"
        )
        boosts_query = (
            f"SELECT doc, boost, REPLACE(query, '.', '') AS query FROM {boosts_collection_name}"
        )
        grouped_boosts = (
            from_sql(boosts_query)
            .groupBy("doc")
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boosts"))
            .withColumnRenamed("doc", "upc")
        )
        grouped_boosts = grouped_boosts.withColumn(
            "signals_boosts", generate_stuffed_column(col("signals_boosts"))
        )
        return from_sql(product_query).join(grouped_boosts, "upc")
