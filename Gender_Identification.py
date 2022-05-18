import requests
import os


#curl -X POST 'https://www.picpurify.com/analyse/1.1' -F 'API_KEY=XXX' -F 'task=porn_moderation,drug_moderation,gore_moderation' -F 'origin_id=xxxxxxxxx' -F 'reference_id=yyyyyyyy' -F 'file_image=@/path/to/local/file.jpg'


picpurify_url = 'https://www.picpurify.com/analyse/1.1'
#img_data = {'file_url': open('https://deadline.com/wp-content/uploads/2019/10/shutterstock_editorial_10434333bm.jpg', 'rb')}
# if you are using non acsii file names, use the following code instead of the previous line
# img_data = {'file_image': (os.path.basename('/path/to/local/file_speciàl_chàr.jpg').encode('ascii', 'replace'),open('/path/to/local/file_speciàl_chàr.jpg'.decode('utf-8'), 'rb').read())}

result_data = requests.post(picpurify_url, data = {"url_image":"https://assets.teenvogue.com/photos/5bec8de88212af3556a4a507/16:9/w_2560%2Cc_limit/_template_triptych_lede.jpg", "API_KEY":"4hhAWQnp6qiA9jDwr5wlgw3XZb3y9DJK", "task":"face_gender_age_detection"})
print(result_data.content)
