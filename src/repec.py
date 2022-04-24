import datetime as dt
import re
from functools import partial

import datazimmer as dz
import pandas as pd
import requests
from atqo import parallel_map
from bs4 import BeautifulSoup
from colassigner import get_all_cols


class NepIndex(dz.IndexBase):
    nid = str


class PaperIndex(dz.IndexBase):
    pid = str


class AuthorIndex(dz.IndexBase):
    aid = str


class NepIssueIndex(dz.IndexBase):
    neid = str


class NepInclusionIndex(dz.IndexBase):
    ind = int
    issue = NepIssueIndex


class NepIssueFeatures(dz.TableFeaturesBase):
    nep = NepIndex
    published = dt.datetime


class NepFeatures(dz.TableFeaturesBase):
    title = str
    info = str


class PaperFeatures(dz.TableFeaturesBase):
    link = str
    year = float
    abstract = str
    title = str
    institution = str


class AuthorFeatures(dz.TableFeaturesBase):
    name = str


class NepInclusionFeatures(dz.TableFeaturesBase):
    paper = PaperIndex


class AuthorshipIndex(dz.IndexBase):
    paper = PaperIndex
    author = AuthorIndex


class KeywordCategorizationFeatures(dz.TableFeaturesBase):
    paper = PaperIndex
    keyword = str


nep_table = dz.ScruTable(NepFeatures, NepIndex)
paper_table = dz.ScruTable(PaperFeatures, PaperIndex)
author_table = dz.ScruTable(AuthorFeatures, AuthorIndex)
authorship_table = dz.ScruTable(index=AuthorshipIndex)
nep_issue_table = dz.ScruTable(NepIssueFeatures, NepIssueIndex)
nep_inclusion_table = dz.ScruTable(NepInclusionFeatures, NepInclusionIndex)

econpaper_base = dz.SourceUrl("https://econpapers.repec.org")
nep_base = dz.SourceUrl("http://nep.repec.org/")
stat_base = dz.SourceUrl("https://logec.repec.org")


@dz.register_data_loader(cron="0 0 * * 0")
def load():
    nep_df = pd.read_html(nep_base)[3].rename(
        columns={"edited by": NepFeatures.info, "access": NepIndex.nid}
    )
    nep_table.replace_records(nep_df)

    latest_collected_issues = (
        nep_issue_table.get_full_df()
        .groupby(NepIssueFeatures.nep.nid)[NepIssueFeatures.published]
        .max()
        .astype(str)
        .to_dict()
    )
    nep_rec_sets = parallel_map(
        partial(get_archive, latest_ones=latest_collected_issues),
        nep_df[NepIndex.nid].tolist(),
        workers=10,
        dist_api="mp",
        pbar=True,
        raise_errors=True,
    )
    paper_rec_df = pd.concat(map(pd.DataFrame, nep_rec_sets)).assign(
        pid=lambda df: df["link"].pipe(paper_link_to_id),
        nepis=lambda df: df["nep"] + "-" + df["published"],
    )
    dump_paper_meta(paper_rec_df["link"].unique())
    nep_issue_table.replace_records(
        paper_rec_df.rename(
            columns={"nepis": NepIssueIndex.neid, "nep": NepIssueFeatures.nep.nid}
        ).drop_duplicates(subset=[NepIssueIndex.neid])
    )
    nep_inclusion_table.replace_records(
        paper_rec_df.rename(
            columns={
                "nepis": NepInclusionIndex.issue.neid,
                "pid": NepInclusionFeatures.paper.pid,
            }
        ).drop_duplicates(subset=get_all_cols(NepInclusionIndex))
    )


# @dz.register_env_creator
def make_envs():
    # TODO with min number of papers
    pass


def get_soup(url):
    return BeautifulSoup(requests.get(url).content, "html5lib")


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
        # pd.read_parquet("/home/borza/mega/data/metascience/repec_paper_meta.p")
        pd.DataFrame(paper_dics)
        .assign(**{PaperIndex.pid: lambda df: paper_link_to_id(df["paper_link"])})
        .rename(columns={"paper_link": PaperFeatures.link})
        .set_index(PaperIndex.pid)
        .rename(
            columns=lambda s: s.replace("citation_", "").replace(
                "technical_report_", ""
            )
        )
    )
    paper_table.replace_records(paper_meta.reindex(get_all_cols(PaperFeatures), axis=1))
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


def get_archive(nep_id, latest_ones):
    recs = []
    archive_soup = get_soup(f"{econpaper_base}/scripts/nep.pf?list={nep_id}")
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
            search_soup = get_soup(url)
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
    soup = get_soup(f"{econpaper_base}{paper_link}")
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
        **{AuthorIndex.aid: lambda df: df.loc[:, 0].str.lower().str.replace(", ", ":")}
    )
    author_table.replace_records(
        base_df.drop_duplicates(subset=[AuthorIndex.aid]).rename(
            columns={0: AuthorFeatures.name}
        )
    )
    authorship_table.replace_records(
        base_df.reset_index().rename(
            columns={
                AuthorIndex.aid: AuthorshipIndex.author.aid,
                PaperIndex.pid: AuthorshipIndex.paper.pid,
            }
        )
    )


def proc_keywords(_):
    pass


def paper_link_to_id(s):
    return s.str.extract(r"/paper/(.*)\.htm")
