import pandas 
import glob 

def all_sessions():
    sessions = pandas.concat([pandas.read_csv(f, compression="gzip")
                          for f in glob.glob("../data/*_sessions.gz")])
    sessions = sessions.sort_values(['query', 'sess_id', 'rank'])
    sessions = sessions.rename(columns={"clicked_doc_id": "doc_id"})
    return sessions

def get_sessions(query="", index=True):
    sessions = all_sessions() 
    sessions = sessions[sessions["query"] == query]
    return sessions if not index else sessions.set_index("sess_id")

def calculate_ctr(sessions):
    click_counts = sessions.groupby("doc_id")["clicked"].sum()
    sess_counts = sessions.groupby("doc_id")["sess_id"].nunique()
    ctrs = click_counts / sess_counts
    return ctrs.sort_values(ascending=False)

def calculate_average_rank(sessions):
    avg_rank = sessions.groupby("doc_id")["rank"].mean()
    return avg_rank.sort_values(ascending=True)

def caclulate_examine_probability(sessions):
    last_click_per_session = sessions.groupby(["clicked", "sess_id"])["rank"].max()[True]
    sessions["last_click_rank"] = last_click_per_session
    sessions["examined"] = sessions["rank"] <= sessions["last_click_rank"]
    return sessions

def calculate_clicked_examined(sessions):
    sessions = caclulate_examine_probability(sessions)
    return sessions[sessions["examined"]] \
        .groupby("doc_id")[["clicked", "examined"]].sum()

def calculate_grade(sessions):
    sessions = calculate_clicked_examined(sessions)
    sessions["grade"] = sessions["clicked"] / sessions["examined"]
    return sessions.sort_values("grade", ascending=False)

def calculate_prior(sessions, prior_grade, prior_weight):
    sessions = calculate_grade(sessions)
    sessions["prior_a"] = prior_grade * prior_weight
    sessions["prior_b"] = (1 - prior_grade) * prior_weight
    return sessions

def calculate_sdbn(sessions, prior_grade=0.3, prior_weight=100):
    sessions = calculate_prior(sessions, prior_grade, prior_weight)
    sessions["posterior_a"] = (sessions["prior_a"] + 
                               sessions["clicked"])
    sessions["posterior_b"] = (sessions["prior_b"] + 
      sessions["examined"] - sessions["clicked"])
    sessions["beta_grade"] = (sessions["posterior_a"] /
      (sessions["posterior_a"] + sessions["posterior_b"]))
    return sessions.sort_values("beta_grade", ascending=False)