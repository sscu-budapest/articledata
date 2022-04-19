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


class NepInclusionFeatures(dz.TableFeaturesBase):
    paper = PaperIndex


nep_table = dz.ScruTable(NepFeatures, NepIndex)
paper_table = dz.ScruTable(PaperFeatures, PaperIndex)
nep_issue_table = dz.ScruTable(NepIssueFeatures, NepIssueIndex)
nep_inclusion_table = dz.ScruTable(NepInclusionFeatures, NepInclusionIndex)

econpaper_base = dz.SourceUrl("https://econpapers.repec.org")
nep_base = dz.SourceUrl("http://nep.repec.org/")


@dz.register_data_loader(cron="0 0 * * 0")
def load():
    nep_df = pd.read_html(nep_base)[3].rename(
        columns={"edited by": NepFeatures.info, "access": NepIndex.nid}
    )
    nep_table.replace_records(nep_df)

    latest_issues = (
        nep_issue_table.get_full_df()
        .groupby(NepIssueFeatures.nep.nid)[NepIssueFeatures.published]
        .max()
        .astype(str)
        .to_dict()
    )
    nep_rec_sets = parallel_map(
        partial(get_archive, latest_ones=latest_issues),
        nep_df[NepIndex.nid].tolist(),
        workers=10,
        dist_api="mp",
        pbar=True,
        raise_errors=True,
    )
    paper_rec_df = pd.concat(map(pd.DataFrame, nep_rec_sets)).assign(
        pid=lambda df: df["link"].str.extract(r"/paper/(.*)\.htm"),
        nepis=lambda df: df["nep"] + "-" + df["published"],
    )
    (
        paper_rec_df.rename(columns={"pid": PaperIndex.pid})
        .drop_duplicates(subset=[PaperIndex.pid])
        .pipe(paper_table.replace_records)
    )
    (
        paper_rec_df.rename(
            columns={"nepis": NepIssueIndex.neid, "nep": NepIssueFeatures.nep.nid}
        )
        .drop_duplicates(subset=[NepIssueIndex.neid])
        .pipe(nep_issue_table.replace_records)
    )
    (
        paper_rec_df.rename(
            columns={
                "nepis": NepInclusionIndex.issue.neid,
                "pid": NepInclusionFeatures.paper.pid,
            }
        )
        .drop_duplicates(subset=get_all_cols(NepInclusionIndex))
        .pipe(nep_inclusion_table.replace_records)
    )


def get_soup(url):
    return BeautifulSoup(requests.get(url).content)


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
