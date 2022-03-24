import datetime as dt

import datazimmer as dz
import pandas as pd
import requests
from bs4 import BeautifulSoup


class PostFeatures(dz.TableFeaturesBase):
    post_id = str
    rank = int
    title = str
    link = str
    sitebit = str
    posted = str
    score = int
    poster = str
    comments = int


post_table = dz.ScruTable(PostFeatures, max_partition_size=10_000)


@dz.register(outputs=[post_table], cron="0 * * * *")
def collect():
    soup = BeautifulSoup(requests.get("https://news.ycombinator.com/").content, "html5lib")
    recs = []
    for tr in soup.find_all("tr", class_="athing"):
        title_a = tr.find("a", class_="titlelink")
        sub_info = tr.find_next("tr")
        last_link = sub_info.find_all("a")[-1]
        rec = {
            PostFeatures.post_id: tr["id"],
            PostFeatures.rank: int(float(tr.find("span", class_="rank").text)),
            PostFeatures.title: title_a.text.strip(),
            PostFeatures.link: title_a["href"],
            PostFeatures.sitebit: getattr(tr.find("span", class_="sitebit"), "text", "").strip(),
            PostFeatures.posted: sub_info.find("span", class_="age")["title"],
            PostFeatures.score: _parseint(sub_info.find("span", class_="score")),
            PostFeatures.poster: getattr(sub_info.find("a", class_="hnuser"), "text", "").strip(),
            PostFeatures.comments: _parseint(last_link) if "comment" in last_link.text else 0,
        }
        recs.append(rec)

    pd.DataFrame(recs).assign(collected=dt.datetime.now()).pipe(post_table.extend)


def _parseint(elem):
    return int("".join(getattr(elem, "text", "0 p").split()[:-1]))
