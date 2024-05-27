def every_other_zipped(lst):
    return zip(lst[0::2],lst[1::2])

def dictify(nl_tups):
    """ Return dict if all keys unique, otherwise
        dont modify """
    as_dict = dict(nl_tups)
    if len(as_dict) == len(nl_tups):
        return as_dict
    return nl_tups

def parse_named_list(lst):
    shallow_tups = [tup for tup in every_other_zipped(lst)]

    nl_as_tups = []

    for tup in shallow_tups:
        if isinstance(tup[1], list):
            tup = (tup[0], parse_named_list(tup[1]))
        nl_as_tups.append(tup)
    return dictify(nl_as_tups)


def parse_termvect_namedlist(lst, field):
    """ Parse the named list and perform some transformations to create consistent
       JSON to parse

        Specifically changing {"positions": ...} to {"positions": [1234,4567]}

       """

    def listify_posns(posn_attrs):
        if isinstance(posn_attrs, dict):
            assert len(posn_attrs) == 1
            return [posn_attrs['position']]
        return [posn_attr[1] for posn_attr in posn_attrs]


    tv_parsed = parse_named_list(lst)
    for doc_id, doc_field_tv in tv_parsed.items():
        for field_name, term_vects in doc_field_tv.items():
            # T
            if field_name == field:
                for term, attrs in term_vects.items():
                    for attr_key, attr_val in attrs.items():
                        if attr_key == 'positions':
                            attrs['positions'] = listify_posns(attr_val)
    return tv_parsed



if __name__ == "__main__":
    solr_nl =  [
		"D100000", [
			"uniqueKey", "D100000",
			"body", [
				"1", [
					"positions", [
						"position", 92,
						"position", 113
					]],
				"2", [
					"positions", [
						"position", 22,
						"position", 413
					]],
				"boo", [
					"positions", [
						"position", 22,
					]]
	        ]]]
    print(repr(parse_termvect_namedlist(solr_nl, 'body')))
