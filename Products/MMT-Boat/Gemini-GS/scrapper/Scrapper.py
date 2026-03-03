from bs4 import BeautifulSoup
import requests
import pandas as pd
import os


class Scrapper:
    def __init__(self, excel_file, output_file="output.xlsx", progress_callback=None):
        self.df = pd.read_excel(excel_file)
        self.progress_callback = progress_callback

        if "scrapped_name" not in self.df.columns:
            self.df["scrapped_name"] = ""
        if "description" not in self.df.columns:
            self.df["description"] = ""

        total_rows = len(self.df)
        
        for idx, row in self.df.iterrows():
            listing_id = str(row.get("Listing_id", "")).strip()
            if not listing_id:
                continue

            if self.progress_callback:
                self.progress_callback({
                    "status": "processing",
                    "current": idx + 1,
                    "total": total_rows,
                    "listing_id": listing_id
                })

            print(f"Processing listing: {listing_id}")

            soup = self.get_soup(listing_id)
            if not soup:
                if self.progress_callback:
                    self.progress_callback({
                        "status": "error",
                        "current": idx + 1,
                        "total": total_rows,
                        "listing_id": listing_id,
                        "message": "Failed to fetch listing"
                    })
                continue

            name, description = self.scrape_name_and_description(soup)
            self.df.at[idx, "scrapped_name"] = name
            self.df.at[idx, "description"] = description

            self.scrape_images(soup, listing_id)
            
            if self.progress_callback:
                self.progress_callback({
                    "status": "completed",
                    "current": idx + 1,
                    "total": total_rows,
                    "listing_id": listing_id,
                    "name": name
                })

        self.df.to_excel(output_file, index=False)
        
        if self.progress_callback:
            self.progress_callback({
                "status": "finished",
                "total": total_rows,
                "output_file": output_file
            })

    def get_soup(self, listing_id):
        url = f"https://www.boatmart.com/listing/{listing_id}"
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception:
            return None

    def scrape_name_and_description(self, soup):
        container = soup.find("div", class_="vdp-main-wrap")
        if not container:
            return "", ""

        name_tag = container.find("h1", class_="tide-typography-title-1")
        description_tag = container.select_one(
            "div.sanitized-html.hyphenated-word-wrap"
        )

        name_text = name_tag.get_text(strip=True) if name_tag else ""
        desc_text = description_tag.get_text(strip=True) if description_tag else ""

        return name_text, desc_text

    def scrape_images(self, soup, listing_id):
        container = soup.find("div", class_="vdp-main-wrap")
        if not container:
            return

        folder = os.path.join("downloaded_images", listing_id)
        os.makedirs(folder, exist_ok=True)

        images = container.select("picture.tide-image img")

        seen = set()
        count = 0

        for img in images:
            if count == 3:
                break

            src = img.get("src")
            if not src:
                continue

            if src.startswith("//"):
                src = "https:" + src

            if src in seen:
                continue

            seen.add(src)
            src = src.replace("width=160", "width=1200").replace(
                "quality=70", "quality=90"
            )

            try:
                img_data = requests.get(src, timeout=10).content
                count += 1
                with open(
                    os.path.join(folder, f"{listing_id}_{count}.webp"), "wb"
                ) as f:
                    f.write(img_data)
            except Exception:
                pass