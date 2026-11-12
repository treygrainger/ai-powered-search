# Full book lab verification (Qdrant 1.20)

Server: `qdrant/qdrant:v1.20.0` + `qdrant-client==1.20.0` with BM25 full-text indexes.

- Notebooks executed: **35**
- PASS: **9**
- FAIL: **26**

## PASS
- `chapters/ch03/1.vectors-and-text-similarity.ipynb` (ok=22, skip=0, 0.0s)
- `chapters/ch03/2.controlling-relevance.ipynb` (ok=4, skip=0, 0.2s)
- `chapters/ch04/1.setting-up-the-retrotech-dataset.ipynb` (ok=7, skip=2, 183.3s)
- `chapters/ch04/2.signals-boosting.ipynb` (ok=6, skip=0, 141.6s)
- `chapters/ch06/2.related-keywords-from-signals.ipynb` (ok=18, skip=0, 238.0s)
- `chapters/ch11/4.train-upload-search-ltr.ipynb` (ok=18, skip=1, 24.8s)
- `chapters/ch12/1.ab-testing-to-active-learning.ipynb` (ok=37, skip=0, 224.3s)
- `chapters/ch14/1.question-answering-visualizer.ipynb` (ok=10, skip=0, 15.7s)
- `chapters/ch14/2.question-answering-data-preparation.ipynb` (ok=11, skip=0, 166.8s)

## FAIL
- `chapters/ch05/1.open-information-extraction.ipynb` (ok=5, err=6, 3.2s)
- `chapters/ch05/2.index-datasets.ipynb` (ok=1, err=3, 49.7s)
- `chapters/ch05/3.semantic-knowledge-graph.ipynb` (ok=6, err=10, 3.1s)
- `chapters/ch06/1.skg-classification-disambiguation.ipynb` (ok=4, err=3, 5.7s)
- `chapters/ch06/3.spell-correction.ipynb` (ok=11, err=2, 288.8s)
- `chapters/ch06/bonus.phrase-detection.ipynb` (ok=10, err=3, 12.1s)
- `chapters/ch06/bonus.related-terms-from-documents.ipynb` (ok=4, err=10, 0.2s)
- `chapters/ch07/1.index-datasets.ipynb` (ok=3, err=3, 6.0s)
- `chapters/ch07/2.semantic-search.ipynb` (ok=6, err=16, 2.3s)
- `chapters/ch08/1.signals-boosting.ipynb` (ok=10, err=15, 1.2s)
- `chapters/ch09/1.personalization.ipynb` (ok=8, err=8, 502.4s)
- `chapters/ch09/2.embedding-based-personalization.ipynb` (ok=11, err=17, 105.2s)
- `chapters/ch10/1.setup-the-movie-db.ipynb` (ok=2, err=1, 8.7s)
- `chapters/ch10/2.judgments-and-logging.ipynb` (ok=4, err=6, 4.5s)
- `chapters/ch10/3.pairwise-transform.ipynb` (ok=0, err=10, 7.4s)
- `chapters/ch10/4.train-and-evaluate-the-model.ipynb` (ok=2, err=12, 5.1s)
- `chapters/ch11/1.click-through-rate-judgments.ipynb` (ok=2, err=15, 0.0s)
- `chapters/ch11/2.sdbn-judgments-to-overcome-position-bias.ipynb` (ok=3, err=8, 0.0s)
- `chapters/ch11/3.SDBN-Confidence-Bias.ipynb` (ok=4, err=12, 0.4s)
- `chapters/ch11/a.synthesize-search-sessions.ipynb` (ok=0, err=4, 0.0s)
- `chapters/ch12/0.setup.ipynb` (ok=3, err=1, 0.1s)
- `chapters/ch14/3.question-answering-fine-tuning.ipynb` (ok=8, err=5, 1.1s)
- `chapters/ch14/4.question-answering-demo-application.ipynb` (ok=6, err=7, 0.4s)
- `chapters/ch15/1.llm-exploration.ipynb` (ok=7, err=10, 0.0s)
- `chapters/ch15/2.multimodal-and-hybrid-search.ipynb` (ok=8, err=12, 4.9s)
- `chapters/ch15/a.generate-movie-embeddings.ipynb` (ok=4, err=1, 0.0s)

## Notes
- Ch1–2 have no notebooks.
- SKG/entity labs still require Solr alongside Qdrant.
- Failures are often missing data downloads, Spark/Solr source connectors, transformers version skew, long training cells, or engine-specific syntax—not always Qdrant adapter bugs.
- BM25 smoke (ch3 controlling relevance ranking) verified Best>Okay>Worst on 1.20.
