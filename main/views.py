from django.shortcuts import render
import requests
from bs4 import BeautifulSoup
from rest_framework.response import Response
from rest_framework.views import status
from rest_framework.decorators import api_view
import os
import pandas as pd
from datetime import datetime
from django.core.mail import EmailMessage
from django.conf import settings
import threading
import urllib.parse
import io
import chardet


URL =  "https://www.google.com/localservices/prolist?g2lbs=ADZRdkvlcpGJMoWTrgDslSQolw5V2LRegZcJFGgwZVdXFEnXT9uc1LU1CcfIHA0g2Eoob3iEY-EEYPyeKxp3VMuR6uTIFX0HYOYJzN9EaU1TMjIYMzPHuUE%3D&hl=en-IN&gl=in&ssta=1&q=List%20of%20personal%20injury%20lawyers%20in%20new%20york&oq=List%20of%20personal%20injury%20lawyers%20in%20new%20york&src=2&serdesk=1&sa=X&ved=2ahUKEwi45MWw0bP_AhXjR2wGHQNjA8UQjGp6BAhUEAE&slp=MgBAAVIECAIgAGAAaAGaAQYKAhcZEAA%3D&scp=ChtnY2lkOnBlcnNvbmFsX2luanVyeV9sYXd5ZXISICIIbWFwIGFyZWEqFA0CRT0YFdJa9tMdWV1QGCWLqQ7UGhdwZXJzb25hbCBpbmp1cnkgbGF3eWVycyoWUGVyc29uYWwgSW5qdXJ5IExhd3llcg%3D%3D"
BASE_URL =  "https://www.google.com/localservices/prolist?g2lbs=ADZRdkvlcpGJMoWTrgDslSQolw5V2LRegZcJFGgwZVdXFEnXT9uc1LU1CcfIHA0g2Eoob3iEY-EEYPyeKxp3VMuR6uTIFX0HYOYJzN9EaU1TMjIYMzPHuUE%3D&hl=en-IN&gl=in&ssta=1"

@api_view(['POST'])
def google_scrapping(request):
    if request.method == 'POST':
        url = request.POST.get('url')
        base_url = request.POST.get('base_url')
        type = request.POST.get('type')
        categories = request.FILES.get('categories')
        cities = request.FILES.get('cities')
        data_list = []
        count = 0
        if type == 'single':
            if url:
                search_list_of_single_data_of_google_map(url,data_list,is_base_url=True)
                csv_name = f"single_url_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
                send_email = threading.Thread(target=send_email_for_clinet, args=(request,data_list,csv_name))
                send_email.start()
                return Response({"msg":"Scrapped Success fully Completed","count":len(data_list)}, status=status.HTTP_200_OK)
            else:
                return Response({"message": "Url is required"}, status=status.HTTP_400_BAD_REQUEST)
        if type == 'multiple':
            if not base_url:
                base_url = BASE_URL
            categories = import_sheets(categories)
            cities = import_sheets(cities)
            for category in categories:
                which_category = category["categories"]
                for city in cities:
                    which_city = city["city"]
                    retrieve_questions(which_category,which_city,base_url,data_list)   
                    csv_name = f"{which_category}_{which_city}_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
                    send_email = threading.Thread(target=send_email_for_clinet, args=(request,data_list,csv_name))
                    send_email.start()
                    count += len(data_list)
                    data_list = []
            return Response({"msg":"Scrapped Success fully Completed","count":count}, status=status.HTTP_200_OK)
        return Response({"msg":"Type Does Not matched"}, status=status.HTTP_400_BAD_REQUEST)




def retrieve_questions(which_category,which_city,base_url,data_list):
    main_question = f"{which_category} in {which_city}"
    encoded_question = urllib.parse.quote(main_question)
    question_url = f"{base_url}{encoded_question}"
    question_url = f"{question_url}&src=2&sa=X&q={encoded_question}&ved=0CAUQjdcJahcKEwj4y-Tf7r3_AhUAAAAAHQAAAAAQFQ"
    responce = search_list_of_single_data_of_google_map(question_url,data_list,is_base_url=True)
    return responce


def search_list_of_single_data_of_google_map(url,data_list,is_base_url=False,permanant_url=None):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        last_side_main_div = soup.find('div', class_='T4LgNb')
        last_side_main_inner3_div = last_side_main_div.find('div', class_='Eli96c') # content div, navigation, result count div
        content_lists_div = last_side_main_inner3_div.find('div', class_='ykYNg')
        jsnames_only_for_contents = content_lists_div.find_all('div', {'jsname': "gam5T"})
        if is_base_url:
            permanant_url = url
        if permanant_url:
            url = permanant_url
        for item in jsnames_only_for_contents:
            if item is not None and 'data-profile-url-path' in item.attrs:
                data_profile_url_path = item['data-profile-url-path']
                single_content_url = data_profile_url_path.replace("/localservices/profile?", "")
                single_data=call_single_data_url(permanant_url,single_content_url)
                if len(single_data) > 0:
                    data_list.append(single_data)
        next_button = last_side_main_inner3_div.find('button', {"aria-label":'Next'})
        next = None
        if next_button:
            pagination = last_side_main_inner3_div.find('div', class_='AIYI7d').text
            next_count = str(pagination).split(' ')[4]
            next = url+"&lci="+next_count
            search_list_of_single_data_of_google_map(next,data_list,permanant_url=permanant_url) # call back to google map api
        return data_list
    except:
        return data_list
    

def call_single_data_url(permanant_url,single_content_url):
    data_dict = {
        "Business Name":"",
        "Phone":"",
        "Email":"",
        "Website":"",
        "Address":"",
        "Hours":"",
        "Services":"",
    }
    try:
        single_data_url = permanant_url+"&"+single_content_url
        response = requests.get(single_data_url)
        single_data_soup = BeautifulSoup(response.content, 'html.parser')
        single_data_main_div = single_data_soup.find('div', class_='eyxqWe') 
        heading = single_data_main_div.find('div', class_='TZpmYe')
        if heading:
            data_dict["Business Name"] = heading.text
        is_full_description = single_data_main_div.find('div', class_='rQJvpe')
        if is_full_description:
            is_over_view = is_full_description.find('div', {'aria-labelledby': "overview"})
            if is_over_view:
                is_over_view_main_description = is_over_view.find('div', class_='bfIbhd')
                if is_over_view_main_description:
                    is_phone = is_over_view_main_description.find('div', class_='eigqqc')
                    if is_phone:
                        phone = is_phone.text
                        data_dict["Phone"] = phone
                    is_website = is_over_view_main_description.find('div', class_='Gx8NHe')
                    if is_website:
                        website = is_website.text
                        data_dict["Website"] = website
                    is_address = is_over_view_main_description.find('div', class_='fccl3c')
                    if is_address:
                        address = is_address.text
                        data_dict["Address"] = address
                    is_hourse = is_over_view_main_description.find('div', class_='LmBKnf')
                    if is_hourse:
                        hour = is_hourse.text
                        data_dict["Hours"] = hour
                    is_servises = is_over_view_main_description.find('div', class_='AQrsxc')
                    if is_servises:
                        servises = is_servises.text
                        if servises:
                            servises = servises.replace('Services:','').split(',')
                            data_dict["Services"] = ','.join(servises)
        return data_dict
    except:
        return data_dict


def send_email_for_clinet(request, data_list, csv_name):
    try:
        if len(data_list) > 0:
            field_names = data_list[0].keys()
            date_frames = pd.DataFrame(data_list, columns=field_names)
            UPLOAD_FOLDER = "media/webscrap_data/"
            if not os.path.exists(UPLOAD_FOLDER):
                os.mkdir(UPLOAD_FOLDER)
            date_folder = datetime.now().strftime("%Y-%m-%d")
            UPLOAD_FOLDER = f"media/webscrap_data/{date_folder}/"
            if not os.path.exists(UPLOAD_FOLDER):
                os.mkdir(UPLOAD_FOLDER)
            date_frames.to_csv(f'{UPLOAD_FOLDER}{csv_name}.csv', index=False)
            to_email = ["muhammedrahilmadathingal@gmail.com"]
            email = EmailMessage(
                subject='CSV File Attachment',
                body='Please find the attached CSV file.',
                from_email='muhammed@zaigoinfotech.com',
                to=to_email,
            )
            csv_file = f'{UPLOAD_FOLDER}{csv_name}.csv'
            with open(csv_file, 'rb') as file:
                csv_data = file.read()
                email.attach(csv_file, csv_data, 'text/csv')
            email.send()
            return 
    except Exception as e:
        print("error :",str(e))



def import_sheets(sheet, orient="records"):
    try:
        import_sheet = sheet.read()
        file_encoding = chardet.detect(import_sheet)['encoding']
        import_sheet = import_sheet.decode(file_encoding)
        df = pd.read_csv(io.StringIO(import_sheet))
        df.columns = map(str.lower, df.columns)
        df.columns = map(str.strip, df.columns)
        df = df.fillna('')
        cleaned_data = df.drop_duplicates(keep='first')
        competency_dict = cleaned_data.to_dict(orient=orient)
    except:
        df = pd.read_excel(sheet)
        df.columns = map(str.lower, df.columns)
        df.columns = map(str.strip, df.columns)
        df = df.fillna('')
        cleaned_data = df.drop_duplicates(keep='first')
        competency_dict = cleaned_data.to_dict(orient=orient)
    return competency_dict



def search_list_datas_of_google_map(url,data_list,is_base_url=False,permanant_url=None):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    last_side_main_div = soup.find('div', class_='T4LgNb')
    last_side_main_inner3_div = last_side_main_div.find('div', class_='Eli96c') # content div, navigation, result count div
    content_lists_div = last_side_main_inner3_div.find('div', class_='ykYNg')
    jsnames_only_for_contents = content_lists_div.find_all('div', {'jsname': "gam5T"})
    for item in jsnames_only_for_contents:     
        data_dict = {}
        main_contents = item.find_all('div', class_='I9iumb') 
        for index,content in enumerate(main_contents):
            if content:
                if index == 0:
                    key = "company name"
                if index == 1:
                    key = "rating"
                if index == 2:
                    key = "address"
                data_dict[key] = content.text
        providers = item.find('div', class_='dLfU4d') 
        if providers:
            provider_text = providers.text
            if "Provides:" in provider_text:
                provider_text = provider_text.replace("Provides:","")
                data_dict["providers"] = str(provider_text).strip()
            if "services" in provider_text:
                data_dict["services"] = str(provider_text).strip()
            if "appointments" in provider_text:
                data_dict["appointments"] = str(provider_text).strip()
        data_list.append(data_dict)
    next_button = last_side_main_inner3_div.find('button', {"aria-label":'Next'})
    next = None
    if next_button:
        if is_base_url:
            permanant_url = url
        if permanant_url:
            url = permanant_url
        pagination = last_side_main_inner3_div.find('div', class_='AIYI7d').text
        next_count = str(pagination).split(' ')[4]
        next = url+"&lci="+next_count
        search_list_datas_of_google_map(next,data_list,permanant_url=permanant_url) # call back to google map api
    return data_list
