import datetime as dt

import datazimmer as dz
import pandas as pd
import requests
from bs4 import BeautifulSoup


class Post(dz.AbstractEntity):
    post_id = str
    rank = int
    title = str
    link = str
    sitebit = str
    posted = str
    score = int
    poster = str
    comments = int
    collected = dt.datetime


post_table = dz.ScruTable(Post, max_partition_size=10_000)


@dz.register(outputs_persist=[post_table], cron="55 * * * *")
def collect():
    soup = BeautifulSoup(
        requests.get("https://news.ycombinator.com/").content, "html5lib"
    )
    recs = []
    for tr in soup.find_all("tr", class_="athing"):
        title_a = tr.find("a", class_="titlelink")
        if title_a is None:
            title_a = tr.find("span", class_="titleline").find("a")
        sub_info = tr.find_next("tr")
        last_link = sub_info.find_all("a")[-1]
        rec = {
            Post.post_id: tr["id"],
            Post.rank: int(float(tr.find("span", class_="rank").text)),
            Post.title: title_a.text.strip(),
            Post.link: title_a["href"],
            Post.sitebit: getattr(
                tr.find("span", class_="sitebit"), "text", ""
            ).strip(),
            Post.posted: sub_info.find("span", class_="age")["title"],
            Post.score: _parseint(sub_info.find("span", class_="score")),
            Post.poster: getattr(
                sub_info.find("a", class_="hnuser"), "text", ""
            ).strip(),
            Post.comments: _parseint(last_link) if "comment" in last_link.text else 0,
        }
        recs.append(rec)
    print(post_table.get_full_df().shape)
    pd.DataFrame(recs).assign(collected=dt.datetime.now()).pipe(post_table.extend)


def _parseint(elem):
    return int("".join(getattr(elem, "text", "0 p").split()[:-1]))
