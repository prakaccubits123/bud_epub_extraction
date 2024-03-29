from bs4 import BeautifulSoup
from bs4 import BeautifulSoup, NavigableString
from utils import (
    timeit,
    mongo_init,
    parse_table,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    generate_unique_id,
)


# change folder and bucket name as required.
bucket_name = "bud-datalake"
# folder_name = "Books/Oct29-1/"
folder_name = "Books/Oct29-1/"
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_harvard")
oct_toc = db.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db.oct_chapters
files_with_error = db.files_with_error
extracted_books = db.extracted_books
publisher_collection = db.publishers


@timeit
def parse_html_to_json(html_content, book, filename):
    # html_content = get_file_object_aws(book, filename)
    soup = BeautifulSoup(html_content, "html.parser")
    # h_tag = get_heading_tags(soup, h_tag=[])
    section_data = extract_data(soup.find("body"), book, filename, section_data=[])
    return section_data


def extract_data(elem, book, filename, section_data=[]):
    for child in elem.children:
        temp = {}
        if isinstance(child, NavigableString):
            if child.strip():
                if section_data:
                    section_data[-1]["content"] += child + " "
                else:
                    temp["title"] = ""
                    temp["content"] = child + " "
                    temp["figures"] = []
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

        elif child.name:
            if child.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                parent_figure = child.find_parent("figure")
                if not parent_figure:
                    if section_data and section_data[-1]["content"].endswith(
                        "{{title}} "
                    ):
                        section_data[-1]["content"] += child.text.strip() + " "
                    else:
                        temp["title"] = child.text.strip()
                        temp["content"] = "{{title}}" + " "
                        temp["figures"] = []
                        temp["tables"] = []
                        temp["code_snippet"] = []
                        temp["equations"] = []

            elif child.name == "img":
                print("figure here from img")
                img = {}
                img["id"] = generate_unique_id()
                aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                img["url"] = aws_path + child["src"]

                parent = child.find_parent("div", class_="image_text")
                if parent:
                    figparent = parent.find_parent("div", class_="illustype_image_text")
                    if figparent:
                        caption_parent = figparent.find("div", class_="caption")
                        if caption_parent:
                            figcap = caption_parent.find("p")
                            if figcap:
                                img["caption"] = figcap.get_text(strip=True)
                                print("this is image_caption", img["caption"])

                if section_data:
                    section_data[-1]["content"] += "{{figure:" + img["id"] + "}} "
                    if "figures" in section_data[-1]:
                        section_data[-1]["figures"].append(img)
                    else:
                        section_data[-1]["figures"] = [img]

                else:
                    temp["title"] = ""
                    temp["content"] = "{{figure:" + img["id"] + "}} "
                    temp["figures"] = [img]
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "table":
                print("table here")
                caption_text = ""
                parent = child.find_parent("div", class_="tableau")
                if parent:
                    tableparent = parent.find("div", class_="caption")
                    if tableparent:
                        tabcaps = tableparent.find_all("p")
                        if tabcaps:
                            caption_text = " ".join(
                                tab_cap.get_text(strip=True) for tab_cap in tabcaps
                            )
                            print("this is table caption", caption_text)

                table_id = generate_unique_id()
                table_data = parse_table(child)
                table = {"id": table_id, "data": table_data, "caption": caption_text}
                if section_data:
                    section_data[-1]["content"] += "{{table:" + table["id"] + "}} "
                    if "tables" in section_data[-1]:
                        section_data[-1]["tables"].append(table)
                        # section_data[-1]['tables'] = [table]
                    else:
                        section_data[-1]["tables"] = [table]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{table:" + table["id"] + "}} "
                    temp["tables"] = [table]
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "pre":
                print("code here")
                code_tags = child.find_all("code")
                code = ""
                if code_tags:
                    code = " ".join(
                        code_tag.get_text(strip=True) for code_tag in code_tags
                    )
                else:
                    code = child.get_text(strip=True)
                code_id = generate_unique_id()
                code_data = {"id": code_id, "code_snippet": code}
                if section_data:
                    section_data[-1]["content"] += "{{code_snippet:" + code_id + "}} "
                    if "code_snippet" in section_data[-1]:
                        section_data[-1]["code_snippet"].append(code_data)

                    else:
                        section_data[-1]["code_snippet"] = [code_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{code_snippet:" + code_id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = [code_data]
                    temp["equations"] = []
            elif child.contents:
                section_data = extract_data(
                    child, book, filename, section_data=section_data
                )
        if temp:
            section_data.append(temp)
    return section_data


@timeit
def get_book_data(book):
    print("Book Name >>> ", book)
    toc = []
    # check if book exists in db toc collection
    db_toc = oct_toc.find_one({"book": book})
    if db_toc:
        toc = db_toc["toc"]
    if not toc:
        error = ""
        try:
            # get table of content
            toc_content = get_file_object_aws(book, "toc.ncx", folder_name, bucket_name)
            if toc_content:
                toc = get_toc_from_ncx(toc_content)
            else:
                toc_content = get_file_object_aws(
                    book, "toc.xhtml", folder_name, bucket_name
                )
                if toc_content:
                    toc = get_toc_from_xhtml(toc_content)
        except Exception as e:
            error = str(e)
            print(f"Error while parsing {book} toc >> {e}")
        if not toc:
            oct_no_toc.insert_one({"book": book, "error": error})
        else:
            oct_toc.insert_one({"book": book, "toc": toc})

    files = []
    order_counter = 0
    prev_filename = None

    for label, content in toc:
        content_split = content.split("#")
        if len(content_split) > 0:
            filename = content_split[0]
            if filename != prev_filename:
                if filename not in files:
                    file_in_error = files_with_error.find_one(
                        {"book": book, "filename": filename}
                    )
                    if file_in_error:
                        files_with_error.delete_one(
                            {"book": book, "filename": filename}
                        )
                    chapter_in_db = oct_chapters.find_one(
                        {"book": book, "filename": filename}
                    )
                    if chapter_in_db:
                        if chapter_in_db["sections"]:
                            continue
                        elif not chapter_in_db["sections"]:
                            oct_chapters.delete_one(
                                {"book": book, "filename": filename}
                            )

                    html_content = get_file_object_aws(
                        book, filename, folder_name, bucket_name
                    )
                    print(filename)
                    if html_content:
                        try:
                            json_data = parse_html_to_json(html_content, book, filename)
                            oct_chapters.insert_one(
                                {
                                    "book": book,
                                    "filename": filename,
                                    "sections": json_data,
                                    "order": order_counter,
                                }
                            )
                            order_counter += 1
                        except Exception as e:
                            print(
                                f"Error while parsing {
                                  filename} html >> {e}"
                            )
                            files_with_error.insert_one(
                                {"book": book, "filename": filename, "error": e}
                            )
                            # clear mongo
                            oct_chapters.delete_many({"book": book})
                    else:
                        print("no html content found : ", filename)
                        files_with_error.insert_one(
                            {
                                "book": book,
                                "filename": filename,
                                "error": "no html content found",
                            }
                        )
                    files.append(filename)
                prev_filename = filename

    book_data = {"book": book, "extraction": "completed"}
    extracted_books.insert_one(book_data)


# taking books from publishers collection and checking if it has pattern (figure tag inside any html file)
# extracted = []
# not_s3 = []
# booknames = []
# for book in publisher_collection.find():
#     if (
#         "publishers" in book
#         and book["publishers"]
#         and book["publishers"][0].startswith("Harvard")
#     ):
#         if "s3_key" in book:
#             s3_key = book["s3_key"]
#             print(s3_key)
#             bookname = book["s3_key"].split("/")[-2]
#             already_extracted = extracted_books.find_one({"book": bookname})
#             if not already_extracted:
#                 booknames.append(bookname)
#             else:
#                 print(f"this {bookname}already extracted")
#                 extracted.append(bookname)
#         else:
#             not_s3.append(book)
# print("total extracted books", len(extracted))
# print("not se", len(not_s3))
# f = open("f.txt", 'w')
# f.write(str(booknames))

# # get_book_data('9781422131077')
# # # # get_book_data("Statistical and Machine Learning Approaches for Network Analysis (9781118346983)")
# # # get_book_data("The One-Page Project Manager for IT Projects (9780470275887)")

# books = ['Difficult Conversations (HBR 20-Minute Manager Series) (9781633690790)', 'Improving Business Processes (9781422172681)', 'Ethical Machines (9781647822828)', 'Get the Right Things Done (9781633691995)', 'The HBR Diversity and Inclusion Collection (5 Books) (9781647822026)', 'Experimentation Works (9781633697119)', 'HBR Guide to Better Business Writing 2nd Edition (9781422183366)', 'True Story_ How to Combine Story and Action to Transform Your Business (9781422187562)', 'Dealing with Difficult People (HBR Emotional Intelligence Series) (9781633696099)', 'The Innovator_s Dilemma (9781422197585)', '9781647821272', '9781625275479', '9781633698840', '9781647821333', '9781647823504', '9781422172063', '9781647821760', '9781633693432', '9781422191439', '9781422187326', '9781647822354', '9781633696570', '9781647820749', '9781647823948', '9781625275288', '9781633697904', '9781625275387', '9781422166451', '9781633692312', 'Work Smarter Rule Your Email (9781422195154)', 'Leadership Presence (HBR Emotional Intelligence Series) (9781633696259)', '9781633699687', '9781422142295', '9781633692596', 'Leading Digital (9781625272485)', 'HBR_s 10 Must Reads on Entrepreneurship and Startups (featuring Bonus Article �Why the Lean Startup Changes Everything� by Steve Blank) (9781633694392)', '9781633699083',
#          '9781422131077', '9781422160848', '9781422171967', '9781633693371', '9781422157138', '9781647821050', 'HBR_s 10 Must Reads on Managing Yourself and Your Career 6-Volume Collection (9781647822040)', 'The Innovator_s Guide to Growth (9781422146033)', 'Fail Better_ Design Smart Mistakes and Succeed Sooner (9781422193457)', 'The Leaders We Need (9781422163603)', 'Good Charts (9781633690714)', 'Build a Successful Business (9781633691964)', 'HBR_s 10 Must Reads on Managing Yourself Vol. 2 (with bonus article _Be Your Own Best Advocate_ by Deborah M. Kolb) (9781647820817)', 'What Happened to Goldman Sachs (9781422194201)', 'Sleeping with Your Smartphone (9781422144060)', 'Breaking Bad Habits (9781633696839)', 'Remix Strategy (9781625270573)', 'Becoming a New Manager (9781422163887)', 'High-Performance Teams (9781633691834)', 'Managing Oneself (9781633693050)', 'Advice for Working Moms (HBR Working Parents Series) (9781647820930)', 'Plugged In_ The Generation Y Guide to Thriving at Work (9781422163665)', 'The Discipline of Teams (9781633691032)', 'The Three-Box Solution Playbook (9781633698314)', 'The Man Who Sold America (9781422161777)', 'The Opposable Mind (9781422148105)', 'How to Communicate Successfully (9781633691612)', 'Peter F. Drucker on Nonprofits and the Public Sector (9781633699588)']
# for book in books:
#     get_book_data(book)
