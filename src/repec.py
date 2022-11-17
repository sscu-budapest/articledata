import datetime as dt
import re
from functools import partial

import datazimmer as dz
import pandas as pd
import requests
from atqo import parallel_map
from bs4 import BeautifulSoup


class Nep(dz.AbstractEntity):
    nid = dz.Index & str
    title = str
    info = str


class Paper(dz.AbstractEntity):
    pid = dz.Index & str

    link = str
    year = dz.Nullable(float)
    abstract = dz.Nullable(str)
    title = str
    institution = dz.Nullable(str)


class Author(dz.AbstractEntity):
    aid = dz.Index & str
    name = str


class NepIssue(dz.AbstractEntity):
    neid = dz.Index & str

    nep = Nep
    published = dt.datetime


class NepInclusion(dz.AbstractEntity):
    ind = dz.Index & int
    issue = dz.Index & NepIssue

    paper = Paper


class Authorship(dz.AbstractEntity):
    paper = dz.Index & Paper
    author = dz.Index & Author


class KeywordCategorization(dz.AbstractEntity):
    paper = Paper
    keyword = str


nep_table = dz.ScruTable(Nep)
paper_table = dz.ScruTable(Paper)
author_table = dz.ScruTable(Author)
authorship_table = dz.ScruTable(Authorship)
nep_issue_table = dz.ScruTable(NepIssue)
nep_inclusion_table = dz.ScruTable(NepInclusion)

econpaper_base = dz.SourceUrl("https://econpapers.repec.org")
nep_base = dz.SourceUrl("http://nep.repec.org/")
stat_base = dz.SourceUrl("https://logec.repec.org")


@dz.register_data_loader
def load():
    nep_df = pd.DataFrame(parse_nep_soup(get_xml_soup(nep_base)))

    nep_table.replace_records(nep_df)

    issue_df = nep_issue_table.get_full_df()
    if issue_df.empty:
        latest_collected_issues = {}
    else:
        latest_collected_issues = (
            issue_df.groupby(NepIssue.nep.nid)[NepIssue.published]
            .max()
            .astype(str)
            .to_dict()
        )
    nep_rec_sets = parallel_map(
        partial(get_archive, latest_ones=latest_collected_issues),
        nep_df[Nep.nid].tolist(),
        workers=10,
        pbar=True,
    )
    paper_rec_df = pd.concat(map(pd.DataFrame, nep_rec_sets)).assign(
        pid=lambda df: df["link"].pipe(paper_link_to_id),
        nepis=lambda df: df["nep"] + "-" + df["published"],
    )
    dump_paper_meta(paper_rec_df["link"].unique())
    nep_issue_table.replace_records(
        paper_rec_df.rename(
            columns={"nepis": NepIssue.neid, "nep": NepIssue.nep.nid}
        ).drop_duplicates(subset=[NepIssue.neid])
    )
    nep_inclusion_table.replace_records(
        paper_rec_df.rename(
            columns={
                "nepis": NepInclusion.issue.neid,
                "pid": NepInclusion.paper.pid,
            }
        ).drop_duplicates(subset=nep_inclusion_table.index_cols)
    )


# @dz.register_env_creator
def make_envs(abstract_chars, min_papers_per_author):
    au_df = (
        authorship_table.get_full_df(env="complete")
        .assign(c=1)
        .groupby("author__aid")
        .transform("sum")
        .loc[lambda df: df["c"] >= min_papers_per_author]
    )
    pids = au_df.index.get_level_values(Authorship.paper.pid).unique().to_numpy()
    aids = au_df.index.get_level_values(Authorship.author.aid).unique().to_numpy()
    paper_df = (
        paper_table.get_full_df()
        .loc[pids, :]
        .assign(**{Paper.abstract: lambda df: df[Paper.abstract].str[:abstract_chars]})
    )
    neinc_df = nep_inclusion_table.get_full_df().loc[
        lambda df: df[NepInclusion.paper.pid].isin(set(pids)), :
    ]
    dz.dump_dfs_to_tables(
        [
            (au_df, authorship_table),
            (paper_df, paper_table),
            (author_table.get_full_df().loc[aids, :], author_table),
            (nep_table.get_full_df(), nep_table),
            (nep_issue_table.get_full_df(), nep_issue_table),
            (neinc_df, nep_inclusion_table),
        ]
    )


def dump_paper_meta(paper_links):
    if not len(paper_links):
        return
    paper_dics = parallel_map(
        get_paper_dic,
        paper_links,
        dist_api="mp",
        raise_errors=True,
        pbar=True,
        workers=12,
    )
    paper_meta = (
        pd.DataFrame(paper_dics)
        .assign(**{Paper.pid: lambda df: paper_link_to_id(df["paper_link"])})
        .rename(columns={"paper_link": Paper.link})
        .set_index(Paper.pid)
        .rename(
            columns=lambda s: s.replace("citation_", "").replace(
                "technical_report_", ""
            )
        )
    )

    paper_table.replace_records(paper_meta.reindex(paper_table.feature_cols, axis=1))
    for usc, procfun in [("authors", proc_authors)]:
        # TODO, ("keywords", proc_keywords)]:
        if usc not in paper_meta.columns:
            continue
        procfun(
            paper_meta[usc]
            .dropna()
            .str.split("; ", expand=True)
            .unstack()
            .dropna()
            .reset_index(level=0, drop=True)
            .reset_index()
            .drop_duplicates()
        )


def get_archive(nep_id, latest_ones: dict):
    recs = []
    archive_soup = get_xml_soup(f"{econpaper_base}/scripts/nep.pf?list={nep_id}")
    latest_issue = latest_ones.get(nep_id, "")
    for a in archive_soup.find_all(
        "a", href=re.compile(r"/scripts/search.pf\?neplist=.*")
    ):
        archive_link = a["href"]
        pub_date = archive_link[-10:]
        if latest_issue > pub_date:
            continue
        url = f"{econpaper_base}{archive_link};iframes=no"
        ind_start = 1
        while True:
            search_soup = get_xml_soup(url)
            a_list = search_soup.find_all("a", href=re.compile("/paper/.*/.*"))
            recs += [
                {
                    "link": a["href"],
                    "nep": nep_id,
                    "nep_arch": archive_link,
                    "ind": ai + ind_start,
                    "published": pub_date,
                }
                for ai, a in enumerate(a_list)
            ]
            next_page_button = search_soup.find(
                "img", class_="rightarrow", src="/right.png"
            )
            ind_start += len(a_list)
            if next_page_button is None:
                break
            url = f"{econpaper_base}{next_page_button.parent['href']}"
    return recs


def get_paper_dic(paper_link):
    soup = get_xml_soup(f"{econpaper_base}{paper_link}")
    stat_link = soup.find("a", href=re.compile(f"{stat_base}/scripts/paperstat.pf.*"))
    meta_dic = {
        m.get("name"): m["content"] for m in soup.find_all("meta") if m.get("name")
    }
    return {
        "stat_link": (stat_link or {}).get("href"),
        **meta_dic,
        "paper_link": paper_link,
    }


def proc_authors(rels):
    base_df = rels.assign(
        **{Author.aid: lambda df: df.loc[:, 0].str.lower().str.replace(", ", ":")}
    )
    author_table.replace_records(
        base_df.drop_duplicates(subset=[Author.aid]).rename(columns={0: Author.name})
    )
    authorship_table.replace_records(
        base_df.reset_index().rename(
            columns={
                Author.aid: Authorship.author.aid,
                Paper.pid: Authorship.paper.pid,
            }
        )
    )


def parse_nep_soup(soup):
    for _nep in soup.find_all("div", class_="nitpo_antem"):
        info_txt = _nep.find_all("span")[2].text
        yield {
            Nep.nid: _nep.find_all("span")[1].text.strip(),
            Nep.title: info_txt.split(",")[0].strip(),
            Nep.info: ", ".join(info_txt.split(", ")[1:]).strip(),
        }


def get_xml_soup(url):
    return BeautifulSoup(requests.get(url).content, "xml")


def proc_keywords(_):
    pass


def paper_link_to_id(s):
    return s.str.extract(r"/paper/(.*)\.htm")
