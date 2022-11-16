import datetime as dt
from typing import Union

import aswan
import datazimmer as dz
import pandas as pd
from bs4 import BeautifulSoup, Tag

main_url = dz.SourceUrl("https://news.ycombinator.com/")


class GiveUp(aswan.RequestHandler):
    process_indefinitely: bool = True

    def is_session_broken(self, _: Union[int, Exception]):
        return False


class RegTop(aswan.RequestSoupHandler):
    def parse(self, soup: BeautifulSoup):

        for tr in soup.find_all("tr", class_="athing")[:5]:
            title_a = tr.find("a", class_="titlelink")
            if title_a is None:
                title_a = tr.find("span", class_="titleline").find("a")
            self.register_links_to_handler([title_a["href"]], GiveUp)

        return soup.encode("utf-8")


class PostDzA(dz.DzAswan):
    name: str = "hackernews"
    cron: str = "55 * * * *"
    starters = {RegTop: [main_url], GiveUp: []}


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


@dz.register_data_loader(extra_deps=[PostDzA])
def collect():
    ap = PostDzA()
    for coll_ev in ap.get_unprocessed_events(RegTop):
        soup = BeautifulSoup(coll_ev.content, "lxml")
        df = pd.DataFrame(map(_parse_tr, soup.find_all("tr", class_="athing")))
        df.assign(collected=coll_ev.cev.iso).pipe(post_table.extend)


def _parse_tr(tr: Tag):
    title_a = tr.find("a", class_="titlelink")
    if title_a is None:
        title_a = tr.find("span", class_="titleline").find("a")
    sub_info = tr.find_next("tr")
    last_link = sub_info.find_all("a")[-1]
    return {
        Post.post_id: tr["id"],
        Post.rank: int(float(tr.find("span", class_="rank").text)),
        Post.title: title_a.text.strip(),
        Post.link: title_a["href"],
        Post.sitebit: getattr(tr.find("span", class_="sitebit"), "text", "").strip(),
        Post.posted: sub_info.find("span", class_="age")["title"],
        Post.score: _parseint(sub_info.find("span", class_="score")),
        Post.poster: getattr(sub_info.find("a", class_="hnuser"), "text", "").strip(),
        Post.comments: _parseint(last_link) if "comment" in last_link.text else 0,
    }


def _parseint(elem):
    return int("".join(getattr(elem, "text", "0 p").split()[:-1]))
