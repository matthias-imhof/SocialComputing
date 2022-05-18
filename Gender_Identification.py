import requests
import os


#curl -X POST 'https://www.picpurify.com/analyse/1.1' -F 'API_KEY=XXX' -F 'task=porn_moderation,drug_moderation,gore_moderation' -F 'origin_id=xxxxxxxxx' -F 'reference_id=yyyyyyyy' -F 'file_image=@/path/to/local/file.jpg'


picpurify_api_keys = ['4hhAWQnp6qiA9jDwr5wlgw3XZb3y9DJK', '9Vgra9ab0ic2EEkGIhuHlSiZ8XWr9UeL']

picpurify_url = 'https://www.picpurify.com/analyse/1.1'


result_data = requests.post(picpurify_url, data = {"url_image":"http://drkernisan.net/wp-content/uploads/bigstock-Beautiful-Portrait-Of-An-Elder-5884730-e1395250928579.jpg", "API_KEY":picpurify_api_keys[0], "task":"face_gender_age_detection"})


print(result_data.content)


