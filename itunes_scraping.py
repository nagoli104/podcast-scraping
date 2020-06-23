from bs4 import BeautifulSoup
import requests
from string import ascii_uppercase
import pickle
import pandas as pd
import numpy as np


# load dedup dictionary
genres_dedup_frames = pickle.load(open("podcasts/dedup_podcasts.pickle", "rb"))

# dict to save failed scraping
failed = {}

# traverse through the keys, get podcast infos and save it in one csv file per genre
for key in genres_dedup_frames:
    
    print(key)
    genre_frame = genres_dedup_frames[key]

    casts = {}
    pc_artist_names = {}

    for index, row in genre_frame.iterrows():
        link = row["link"]
        pc_id = row["id"]

        # get
        pc_page = requests.get(link)
        soup_pc = BeautifulSoup(pc_page.content, "html.parser")

        # first check if an episode was published in 2019 or later
        # information is hidden in div third div (refers to latest episode)
        # there are some discrepancies of episodes listed in the overview page and entries
        # that have actually pages
        try:
            div_class_of_interest = soup_pc.find_all("div", class_="l-row")[2]

            latest_year = int(div_class_of_interest.div.time["datetime"].split("-")[0])

            if latest_year >= 2019:

                # find genre, artist and artis id
                div_class_of_interest = soup_pc.find_all(
                    "div", class_="l-column small-7 medium-12 small-valign-top"
                )

                # not all artists have an id and/or a artist page
                try:
                    artist_string = div_class_of_interest[0].h1.a.contents
                    artist_string = " ".join(artist_string[0].split())
                    artist_link = div_class_of_interest[0].h1.a["href"]

                    try:
                        artist_id = artist_link.split("/")[6]
                    except:
                        artist_id = artist_link.split("/")[5]

                    # add it to artist dict
                    artist_names[artist_id] = artist_string
                    artist_links[artist_id] = artist_link

                    # add artist name to podcast dict

                except:
                    # only get artist name to store in podcast names dict
                    artist_string = div_class_of_interest[0].h1.contents[1].contents
                    artist_string = " ".join(artist_string[0].split())

                # get genre
                genre_string = div_class_of_interest[0].li.li.contents
                genre_string = " ".join(genre_string[0].split())

                # get title, description out of soup object and cut out unicode thingy
                title = soup_pc.find("meta", {"name": "apple:title"})[
                    "content"
                ].replace("\u200E", "")
                description = soup_pc.find("meta", {"name": "apple:description"})[
                    "content"
                ].replace("\u200E", "")

                casts[pc_id] = np.array(
                    [pc_id, title, description, link, artist_string, genre_string]
                )

        except:
            print(key, ":", link)
            failed[pc_id] = link

    genre_data_frame = pd.DataFrame([casts[key] for key in casts.keys()])
    genre_data_frame.columns = [
        "itunes_id",
        "name",
        "description",
        "itunes_link",
        "artist_name",
        "genre",
    ]

    # save it under file named after the genre
    save_name = "podcasts/" + key.split(" ")[0]

    genre_data_frame.to_csv(save_name + ".csv")

    with open(save_name + ".pickle", "wb") as f:
        pickle.dump(genre_data_frame, f)

# also save the failed dict, in the hope cases can be resolved later
with open("podcasts/failed_podcasts.pickle", "wb") as f:
    pickle.dump(failed, f)
