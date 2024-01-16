import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import os
from PIL import Image
from pix2tex.cli import LatexOCR
from extract_epub_table import process_book_page
from bs4 import BeautifulSoup, NavigableString
from utils import (
    timeit,
    mongo_init,
    parse_table,
    get_s3,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    generate_unique_id,
)

latex_ocr = LatexOCR()

# change folder and bucket name as required.
bucket_name = "bud-datalake"
folder_name = "Books/Oct29-1/"
# folder_name = "Books/Oct29-Wiley/"/
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_testing")
db2 = mongo_init("epub_wiley3")
oct_toc = db2.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db2.oct_chapters
files_with_error = db.files_with_error
extracted_books = db.extracted_books
publisher_collection = db.publishers


def download_aws_image(key, book):
    try:
        book_folder = os.path.join(folder_name, book)
        if not os.path.exists(book_folder):
            os.makedirs(book_folder)
        local_path = os.path.join(book_folder, os.path.basename(key))
        s3 = get_s3()
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)
        return None


def download_epub_from_s3(bookname, s3_key):
    try:
        local_path = os.path.abspath(os.path.join(folder_name, f"{bookname}.epub"))
        os.makedirs(folder_name, exist_ok=True)
        s3 = get_s3()
        s3.download_file(bucket_name, s3_key, local_path)
        return local_path
    except Exception as e:
        print(e)
        return None

def convert_to_jpg(image_path):
    # Get the file extension
    _, extension = os.path.splitext(image_path)
    print(_)
    print(extension)
    # Check if the extension is not in the specified formats (".jpg", ".png", ".jpeg")
    if extension.lower() not in {".jpg", ".png", ".jpeg"}:
        # Convert the image to JPG format
        jpg_path = image_path.replace(extension, ".jpg")
        convert_image_to_jpg(image_path, jpg_path)
        return jpg_path
    else:
        return image_path

def convert_image_to_jpg(input_path, output_path):
    try:
        with Image.open(input_path) as img:
            # Convert the image to RGB mode
            rgb_image = img.convert('RGB')
            rgb_image.save(output_path, 'JPEG')
    except Exception as e:
        print("Error while converting image to JPEG:", e)

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
                img['caption']=''
                parent = child.find_parent("div", class_='str1')
                parent2= child.find_parent('div', class_='center')
                parent3= child.find_parent('div', class_='figure')
                
                if parent:
                    figparent = parent.find_parent("div", class_="center")
                    if figparent:
                        figcaption = figparent.find("h5", class_="docFigureTitle")
                        if figcaption:
                            img["caption"] = figcaption.get_text(strip=True)
                            print("this is figure caption", img['caption'])

                elif parent2:
                    figcaption = parent2.find("h5", class_="docFigureTitle")
                    if figcaption:
                        img["caption"] = figcaption.get_text(strip=True)
                        print("this is figure caption", img['caption'])

                elif parent3:
                    figcaption = parent3.find("p", class_="figurecaption")
                    if figcaption:
                        img["caption"] = figcaption.get_text(strip=True)
                        print("this is figure caption", img['caption'])

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
            
            elif (child.name == "p" and child.find("img") and not child.get("class")) or (child.name == "div" and child.find("img") and 'graphic' in child.get('class',[])) :
                print("table here from p extraction from image")
                table_img = child.find('img')
                aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                image_path = aws_path + table_img["src"]
                caption_text = ""
                table_id = generate_unique_id()
                previous_sibling= child.find_previous_sibling()
                if previous_sibling:
                    prev_sib_class=previous_sibling.get('class',[''])[0]
                    if prev_sib_class=="tablecaption":
                        caption_text= previous_sibling.get_text(strip=True)
                        print("this is table caption", caption_text)

                if table_img and caption_text!="":
                    img_key = image_path.replace(s3_base_url + "/", "")
                    table_image_path1 = download_aws_image(img_key, book)
                    print(table_image_path1)
                    if not table_image_path1:
                        continue
                    try:
                        table_image_path = convert_to_jpg(table_image_path1)
                        table_data = process_book_page(table_image_path)
                    except Exception as e:
                        print("error while extrcating table using bud-ocr", e)
                        continue
                    if os.path.exists(table_image_path):
                        os.remove(table_image_path)
                    if os.path.exists(table_image_path1):
                        os.remove(table_image_path1)

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
                else:
                    img={'id':generate_unique_id(), 'url':image_path, 'caption':''}

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
                tab_cap= child.find('caption')
                parent = child.find_parent("div", class_="table-contents")
                if tab_cap:
                    caption_text=tab_cap.get_text(strip=True)
                    print("this is table caption", caption_text)    
                elif parent:
                    tableparent = parent.find_parent("div", class_="table")
                    if tableparent:
                        tabcap = tableparent.find("p", "title")
                        if tabcap:
                            caption_text = tabcap.get_text(strip=True)
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

            # code oreilly publication
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
                            print(f"Error while parsing {filename} html >> {e}")
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


def find_figure_tag_in_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    div_with_figure_class = soup.find_all("p", class_="text")
    return div_with_figure_class


def get_html_from_epub(epub_path):
    book = epub.read_epub(epub_path)
    # Iterate through items in the EPUB book
    for item in book.get_items():
        # Check if the item is of type 'text'
        if item.get_type() == ebooklib.ITEM_DOCUMENT:
            # Extract the HTML content
            html_content = item.get_content().decode("utf-8", "ignore")

            # Find figure tags in the HTML content
            figure_tags = find_figure_tag_in_html(html_content)

            # If figure tags are found, return the first one and break the loop
            if figure_tags:
                return figure_tags[0]
    # Return None if no figure tags are found
    return None

# extracted = []
# not_extracted = []
# books = get_all_books_names(bucket_name, folder_name)
# print(len(books))

# book_with_divcenter_tags = []
# books_with_p_text_tags = []
# for num, book in enumerate(books):
#     already_extracted = extracted_books.find_one({"book": book})
#     s3_key = f"{folder_name}{book}/{book}.epub"
#     # print(s3_key)
#     print(num)
#     if not already_extracted:
#         print("e")
#         epub_path = download_epub_from_s3(book, s3_key)
#         if not epub_path:
#             continue
#         try:
#             figure_tag = get_html_from_epub(epub_path)
#         except Exception as e:
#             print("error while identify figure tag", e)
#             continue
#         if figure_tag:
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#                 print("figure tag found")
#                 books_with_p_text_tags.append(book)
#                 # get_book_data(book)
#         else:
#             print("no figure tag")
#             book_with_divcenter_tags.append(book)
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#     else:
#         print("already extracted")


# # print("total books", len(books))
# print("total with div center", len(book_with_divcenter_tags))
# f = open("wileydivcenter.txt", "w")
# f.write(str(book_with_divcenter_tags))

# print("total with ptext", len(books_with_p_text_tags))

# f = open("wileyptext.txt", "w")
# f.write(str(books_with_p_text_tags))
# books=['9780470008614', '9780470011942', '9780470012734', '9780470012864', '9780470019061', '9780470047019', '9780470049297', '9780470051054', '9780470052211', '9780470059029', '9780470061961', '9780470068564', '9780470080221', '9780470092910', '9780470100288', '9780470108888', '9780470112328', '9780470113455', '9780470114346', '9780470114780', '9780470116494', '9780470119556', '9780470122440', '9780470122457', '9780470124178', '9780470126745', '9780470127698', '9780470128725', '9780470129463', '9780470131961', '9780470132319', '9780470134856', '9780470135075', '9780470135136', '9780470138946', '9780470139707', '9780470146927', '9780470147689', '9780470149287', '9780470152621', '9780470169308', '9780470171257', '9780470177136', '9780470183649', '9780470191163', '9780470208236', '9780470222898', '9780470228562', '9780470237915', '9780470238264', '9780470238387', '9780470259252', '9780470260364', '9780470260913', '9780470267554', '9780470267622', '9780470275849', '9780470279496', '9780470285350', '9780470286425', '9780470287156', '9780470292990', '9780470300961', '9780470379288', '9780470382059', '9780470383780', '9780470389683', '9780470395356', '9780470398111', '9780470398364', '9780470398517', '9780470398548', '9780470402375', '9780470405680', '9780470406786', '9780470407356', '9780470407769', '9780470409503', '9780470410547', '9780470410974', '9780470415719', '9780470419571', '9780470422182', '9780470428733', '9780470429037', '9780470432068', '9780470437001', '9780470438473', '9780470440735', '9780470442937', '9780470443330', '9780470443378', '9780470445075', '9780470450376', '9780470452592', '9780470453117', '9780470453650', '9780470455104', '9780470457412', '9780470457641', '9780470457993', '9780470458402', '9780470460122', '9780470464441', '9780470465226', '9780470467183', '9780470471296', '9780470472750', '9780470473238', '9780470474235', '9780470475355', '9780470477236', '9780470478196', '9780470478349', '9780470481097', '9780470481554', '9780470481790', '9780470482551', '9780470483442', '9780470485859', '9780470487846', '9780470488126', '9780470488409', '9780470495858', '9780470496572', '9780470497081', '9780470497104', '9780470498866', '9780470502303', '9780470502952', '9780470503829', '9780470504567', '9780470504987', '9780470505144', '9780470505366', '9780470506912', '9780470508022', '9780470509531', '9780470515044', '9780470515051', '9780470516355', '9780470522394', '9780470524916', '9780470525999', '9780470526118', '9780470526323', '9780470526705', '9780470528051', '9780470528495', '9780470528822', '9780470529720', '9780470530917', '9780470533994', '9780470534915', '9780470538135', '9780470539958', '9780470547816', '9780470550106', '9780470554753', '9780470557297', '9780470557457', '9780470558416', '9780470559901', '9780470563410', '9780470563939', '9780470565070', '9780470567593', '9780470569528', '9780470570715', '9780470570937', '9780470571439', '9780470579893', '9780470580097', '9780470587614', '9780470591260', '9780470596180', '9780470597149', '9780470598771', '9780470599068', '9780470599129', '9780470599150', '9780470599167', '9780470599174', '9780470599266', '9780470599273', '9780470599334', '9780470599815', '9780470601785', '9780470604540', '9780470608418', '9780470609248', '9780470609798', '9780470612750', '9780470614181', '9780470615188', '9780470615287', '9780470616536', '9780470623268', '9780470624159', '9780470625743', '9780470630037', '9780470631492', '9780470632017', '9780470634189', '9780470635520', '9780470637142', '9780470637340', '9780470639535', '9780470640043', '9780470642047', '9780470643471', '9780470643631', '9780470643990', '9780470645956', '9780470647929', '9780470660973', '9780470662120', '9780470662427', '9780470664919', '9780470676547', '9780470684580', '9780470684931', '9780470684962', '9780470685914', '9780470687673', '9780470688670', '9780470689868', '9780470697344', '9780470711880', '9780470711958', '9780470712030', '9780470722992', '9780470724019', '9780470726457', '9780470737606', '9780470745922', '9780470747612', '9780470748824', '9780470758106', '9780470767849', '9780470775639', '9780470826461', '9780470827642', '9780470829752', '9780470839027', '9780470874363', '9780470876411', '9780470878989', '9780470879610', '9780470885864', '9780470888018', '9780470888032', '9780470888681', '9780470891742', '9780470892381', '9780470893418', '9780470893432', '9780470900529', '9780470907399', '9780470912614', '9780470915844', '9780470915851', '9780470918425', '9780470921517', '9780470922101', '9780470923115', '9780470927625', '9780470929834', '9780470933114', '9780470934111', '9780470944561', '9780470947807', '9780470971543', '9780470976289', '9780470977347', '9780471023296', '9780471223986', '9780471267157', '9780471281139', '9780471281177', '9780471356141', '9780471356523', '9780471369462', '9780471412540', '9780471460527', '9780471469124', '9780471485247', '9780471642343', '9780471646280', '9780471649847', '9780471701460', '9780471703082', '9780471705451', '9780471720867', '9780471728009', '9780471741213', '9780471741251', '9780471746867', '9780471777090', '9780471779308', '9780471790174', '9780471973171', '9780730304791', '9780730381990', '9780764596360', '9780787982003', '9780870519727', '9780870519970', '9781118002766', '9781118003930', '9781118005576', '9781118017647', '9781118018965', '9781118022238', '9781118023570', '9781118023938', '9781118023952', '9781118028360', '9781118040850', '9781118044667', '9781118044926', '9781118044940', '9781118045053', '9781118045268', '9781118045589', '9781118045701', '9781118046210', '9781118046470', '9781118058480', '9781118061541', '9781118064085', '9781118067567', '9781118075418', '9781118077061', '9781118084205', '9781118084236', '9781118086681', '9781118086773', '9781118090268', '9781118094280', '9781118095461', '9781118096536', '9781118098363', '9781118098585', '9781118108260', '9781118111970', '9781118119662', '9781118121719', '9781118128770', '9781118145456', '9781118153840', '9781118157831', '9781118160336', '9781118176283', '9781118176351', '9781118188217', '9781118188514', '9781118188569', '9781118197974', '9781118198629', '9781118199558', '9781118203163', '9781118208786', '9781118210321', '9781118210345', '9781118210505', '9781118210567', '9781118211007', '9781118211588', '9781118217221', '9781118218983', '9781118219263', '9781118228982', '9781118232552', '9781118233061', '9781118233382', '9781118233429', '9781118233825', '9781118234013', '9781118234549', '9781118234730', '9781118234761', '9781118234808', '9781118235126', '9781118235133', '9781118235201', '9781118235225', '9781118235454', '9781118235515', '9781118235560', '9781118235607', '9781118235614', '9781118235935', '9781118236093', '9781118236376', '9781118236406', '9781118236840', '9781118236864', '9781118236963', '9781118236994', '9781118237014', '9781118237106', '9781118237335', '9781118237397', '9781118237403', '9781118237410', '9781118237441', '9781118237458', '9781118237533', '9781118237557', '9781118237564', '9781118237625', '9781118237960', '9781118238141', '9781118238219', '9781118238417', '9781118238820', '9781118238912', '9781118238936', '9781118239049', '9781118239155', '9781118239230', '9781118239261', '9781118239285', '9781118239438', '9781118239445', '9781118239490', '9781118239568', '9781118239643', '9781118239667', '9781118239698', '9781118239728', '9781118239759', '9781118239773', '9781118239841', '9781118239988', '9781118240052', '9781118240069', '9781118240113', '9781118240212', '9781118240236', '9781118240434', '9781118240557', '9781118240700', '9781118240748', '9781118240755', '9781118240779', '9781118240908', '9781118240946', '9781118240960', '9781118241325', '9781118243046', '9781118243206', '9781118243268', '9781118247082', '9781118247105', '9781118247228', '9781118252871', '9781118259535', '9781118270073', '9781118271926', '9781118273661', '9781118274316', '9781118275467', '9781118278116', '9781118281987', '9781118282175', '9781118282328', '9781118282342', '9781118282380', '9781118282632', '9781118282786', '9781118282793', '9781118282885', '9781118283035', '9781118283172', '9781118283240', '9781118283493', '9781118283615', '9781118283639', '9781118290521', '9781118300190', '9781118308400', '9781118309636', '9781118309803', '9781118310038', '9781118310205', '9781118311011', '9781118314258', '9781118314333', '9781118316191', '9781118316665', '9781118321638', '9781118330340', '9781118330630', '9781118330692', '9781118331804', '9781118331828', '9781118331842', '9781118331866', '9781118331873', '9781118331897', '9781118336540', '9781118337042', '9781118339206', '9781118339244', '9781118339329', '9781118339404', '9781118340455', '9781118343777', '9781118344903', '9781118345207', '9781118345597', '9781118346983', '9781118347898', '9781118348130', '9781118349199', '9781118349571', '9781118350065', '9781118352489', '9781118352816', '9781118354230', '9781118356302', '9781118358078', '9781118359402', '9781118360118', '9781118361153', '9781118364246', '9781118370230', '9781118378458', '9781118380659', '9781118383780', '9781118385494', '9781118385524', '9781118387351', '9781118387856', '9781118388990', '9781118391389', '9781118391730', '9781118394236', '9781118394328', '9781118394533', '9781118397046', '9781118399422', '9781118400135', '9781118400708', '9781118406359', '9781118407110', '9781118408001', '9781118408148', '9781118410097', '9781118410790', '9781118410844', '9781118411186', '9781118414620', '9781118414712', '9781118416341', '9781118416464', '9781118416624', '9781118416679', '9781118416716', '9781118416730', '9781118416747', '9781118416846', '9781118416853', '9781118416877', '9781118416945', '9781118416969', '9781118416990', '9781118417058', '9781118417089', '9781118417096', '9781118417133', '9781118417195', '9781118417218', '9781118417249', '9781118417317', '9781118417393', '9781118417423', '9781118417447', '9781118417461', '9781118417522', '9781118417539', '9781118417553', '9781118417607', '9781118417669', '9781118417706', '9781118417782', '9781118417850', '9781118417874', '9781118421314', '9781118421352', '9781118421413', '9781118421451', '9781118421499', '9781118421635', '9781118421741', '9781118425152', '9781118431429', '9781118431498', '9781118431603', '9781118431795', '9781118431948', '9781118431955', '9781118432693', '9781118432754', '9781118434505', '9781118438879', '9781118439258', '9781118442319', '9781118443415', '9781118445006', '9781118447284', '9781118448960', '9781118449974', '9781118452936', '9781118454169', '9781118454947', '9781118457191', '9781118457856', '9781118459799', '9781118460634', '9781118461136', '9781118461570', '9781118461679', '9781118461808', '9781118461891', '9781118462126', '9781118462133', '9781118463932', '9781118463994', '9781118464014', '9781118464496', '9781118465172', '9781118467008', '9781118468647', '9781118469057', '9781118478318', '9781118479780', '9781118481868', '9781118483312', '9781118483817', '9781118483855', '9781118484708', '9781118490303', '9781118490358', '9781118490402', '9781118490433', '9781118490457', '9781118493861', '9781118494165', '9781118494196', '9781118496367', '9781118497579', '9781118498484', '9781118498682', '9781118498996', '9781118501344', '9781118502662', '9781118504024', '9781118505212', '9781118505298', '9781118505601', '9781118506172', '9781118506691', '9781118506998', '9781118508756', '9781118509074', '9781118512722', '9781118513668', '9781118515693', '9781118515846', '9781118516096', '9781118518502', '9781118519271', '9781118523162', '9781118524404', '9781118524510', '9781118526170', '9781118526217', '9781118526309', '9781118526538', '9781118531099', '9781118536940', '9781118537435', '9781118539057', '9781118541432', '9781118546604', '9781118548318', '9781118548509', '9781118548899', '9781118550274', '9781118550373', '9781118550434', '9781118551585', '9781118551998', '9781118553084', '9781118555484', '9781118558690', '9781118558867', '9781118559284', '9781118559444', '9781118559840', '9781118562741', '9781118562802', '9781118563069', '9781118563083', '9781118563274', '9781118566053', '9781118566169', '9781118566206', '9781118567623', '9781118569788', '9781118569856', '9781118570081', '9781118570388', '9781118572917', '9781118573082', '9781118574355', '9781118574591', '9781118577226', '9781118579053', '9781118579596', '9781118579893', '9781118580400', '9781118580622', '9781118580660', '9781118584583', '9781118585726', '9781118586136', '9781118586242', '9781118586341', '9781118586358', '9781118586426', '9781118586570', '9781118587171', '9781118588017', '9781118588192', '9781118588543', '9781118589601', '9781118590386', '9781118596753', '9781118597170', '9781118599860', '9781118600092', '9781118600986', '9781118601136', '9781118601143', '9781118601723', '9781118601730', '9781118602171', '9781118602805', '9781118603246', '9781118603321', '9781118603512', '9781118604403', '9781118605295', '9781118605448', '9781118605820', '9781118606292', '9781118607428', '9781118610848', '9781118611180', '9781118611357', '9781118613160', '9781118613252', '9781118614099', '9781118614198', '9781118614471', '9781118614617', '9781118616376', '9781118616420', '9781118616628', '9781118616956', '9781118617267', '9781118618714', '9781118620472', '9781118620632', '9781118622643', '9781118627785', '9781118628584', '9781118631706', '9781118631799', '9781118632222', '9781118637296', '9781118637517', '9781118637586', '9781118637654', '9781118639771', '9781118639924', '9781118641316', '9781118641354', '9781118641392', '9781118642320', '9781118646366', '9781118647844', '9781118648247', '9781118648902', '9781118648957', '9781118649206', '9781118649381', '9781118651919', '9781118652770', '9781118653319', '9781118653579', '9781118654811', '9781118654934', '9781118659700', '9781118659762', '9781118659960', '9781118661260', '9781118673010', '9781118674741', '9781118677698', '9781118679500', '9781118680094', '9781118686133', '9781118688434', '9781118696637', '9781118698860', '9781118698884', '9781118705025', '9781118705285', '9781118706336', '9781118708750', '9781118708774', '9781118714324', '9781118715987', '9781118716168', '9781118716212', '9781118716250', '9781118716281', '9781118718520', '9781118719800', '9781118722336', '9781118725177', '9781118725764', '9781118727027', '9781118728390', '9781118729311', '9781118729366', '9781118731529', '9781118731918', '9781118734025', '9781118735572', '9781118735589', '9781118737781', '9781118739204', '9781118739945', '9781118740002', '9781118741061', '9781118744253', '9781118744468', '9781118744482', '9781118744932', '9781118745274', '9781118745533', '9781118746134', '9781118747155', '9781118748688', '9781118754023', '9781118756683', '9781118757963', '9781118758519', '9781118759707', '9781118759806', '9781118760154', '9781118760727', '9781118761632', '9781118761861', '9781118763186', '9781118764039', '9781118765357', '9781118770139', '9781118785348', '9781118786017', '9781118787397', '9781118790328', '9781118790519', '9781118790823', '9781118793824', '9781118793992', '9781118794166', '9781118794777', '9781118796863', '9781118801406', '9781118808801', '9781118810071', '9781118813812', '9781118814864', '9781118817520', '9781118820247', '9781118825402', '9781118825716', '9781118826461', '9781118828489', '9781118830963', '9781118833384', '9781118838914', '9781118841303', '9781118841549', '9781118841808', '9781118849101', '9781118853962', '9781118854228', '9781118854297', '9781118856185', '9781118858387', '9781118862964', '9781118863442', '9781118863572', '9781118863657', '9781118863664', '9781118865620', '9781118865729', '9781118871232', '9781118872888', '9781118875629', '9781118879665', '9781118881040', '9781118891582', '9781118898970', '9781118899564', '9781118928936', '9781118928967', '9781118930953', '9781118965160', '9781118965429', '9781118965498', '9781118965511', '9781118965535', '9781118982051', '9781118984338', '9781118984512', '9781119009139', '9781119043607', '9781119068594', '9781119102359', '9781119129981', '9781119237921', '9781119247586', '9781119250722', '9781119284956', '9781119430261', '9781119940302', '9781119940531', '9781119940906', '9781119940968', '9781119941675', '9781119942108', '9781119942641', '9781119942719', '9781119943464', '9781119943556', '9781119943709', '9781119945765', '9781119945840', '9781119950240', '9781119950554', '9781119952817', '9781119956525', '9781119960362', '9781119960423', '9781119960461', '9781119960591', '9781119960652', '9781119960676', '9781119960881', '9781119961321', '9781119961550', '9781119963028', '9781119963103', '9781119963233', '9781119964308', '9781119965343', '9781119967842', '9781119969075', '9781119969402', '9781119970484', '9781119971917', '9781119972723', '9781119973683', '9781119973751', '9781119973782', '9781119973829', '9781119973850', '9781119973881', '9781119974291', '9781119975908', '9781119976189', '9781119976417', '9781119977490', '9781119977810', '9781119977995', '9781119978022', '9781119978503', '9781119978626', '9781119978640', '9781119979289', '9781119990253', '9781119990772', '9781119991366', '9781119991458', '9781119991519', '9781119991908', '9781119991915', '9781119992653', '9781119992776', '9781119992936', '9781119993063', '9781119993568', '9781119993674', '9781119993988', '9781119994350', '9781119994947', '9781119994985', '9781119995005', '9781119995081', '9781119995104', '9781119995241', '9781119995289', '9781119995470', '9781119995814', '9781119995968', '9781119998952', '9781405127912', '9781405142793', '9781405160490', '9781444317237', '9781444333381', '9781444354324', '9781444394726', '9781444395426', '9781576601518', '9781576601891', '9781576602430', '9781576603079', '9781576603109', '9781576603161', '9781576603277', '9781576603314', '9781576603475', '9781576603482', '9781576603598', '9781576603604', '9781576603666', '9781848213319', '9781848217553', '9781848217980', '9781848218017', '9781848219229', '9781937352356', '9781941651575', '9783527654963']
books=['The Invisible Employee (9780470560211)', 'Essentials of Knowledge Management (9780471281139)', 'Microsoft® SharePoint® Server 2007 Bible (9780470008614)', 'Nature Photography Photo Workshop (9780470534915)', 'Doing Business in China For Dummies® (9780470049297)']
extracted=[]
for book_num, book in enumerate(books):
    already_extracted = extracted_books.find_one({"book": book})
    print("book number", book_num)
    print("books remaining", (len(books)-book_num))
    if not already_extracted:
        get_book_data(book)
    else:
        print("this book already extracted", book)
        extracted.append(book)

print("total books already extracted",len(extracted))


