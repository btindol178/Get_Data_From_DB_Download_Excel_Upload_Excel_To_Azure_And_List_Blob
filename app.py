from flask import Flask ,render_template,request,redirect,url_for,send_file
#from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
import pyodbc          
import pandas as pd
import os
from datetime import datetime   
from flask_sqlalchemy import SQLAlchemy

import io
from io import BytesIO
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient
########################################################################################################################
# Upload to blob 
connect_str = '<connection string> '

print("The connection string is: ",connect_str)

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
print("The blob service client is: ",blob_service_client)

###########################################################################################################################
# list blobs 
from azure.storage.blob import BlobServiceClient


container_name2 = "form-recognizer"
connect_str2 = "<connection string> "

blob_service_client2 = BlobServiceClient.from_connection_string(conn_str=connect_str2)
##########################################################################################################################
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['SECRET_KEY'] = 'sdflksjslksajfsda' # NOT KOSSURE BUT FOR NOW

# Get current path
path = os.getcwd()
# file Upload
UPLOAD_FOLDER = os.path.join(path, 'uploads')

# Make directory if uploads is not exists
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# Run app through sql alchemy and marshmallow 
db = SQLAlchemy(app)

# Create tables at first
@app.before_first_request
def create_tables():
    db.create_all() 


conn_str = ('DRIVER={SQL Server};'
    'SERVER=server;'
    'DATABASE=database;'
    'Trusted_Connection=yes;')
    
cnxn = pyodbc.connect(conn_str)

@app.route("/home",methods =['GET','POST'])
def home():
    df = "select top 10 from database"
    predata = pd.read_sql(df, cnxn)
    print(predata)
    tables = predata.to_html(classes='table table-striped', header="true", index=False)
    if request.method == "POST":
   
        df = "select top 10 from database"
        predata = pd.read_sql(df, cnxn)
        print(predata)
        tables = predata.to_html(classes='table table-striped', header="true", index=False)

        output = BytesIO()

        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            predata.to_excel(writer, sheet_name="Sheet1")

        output.seek(0)

        return send_file(output, attachment_filename="df.xlsx", as_attachment=True) 
    return render_template("index.html",tables=tables)


@app.route("/",methods =['GET','POST'])
@app.route("/download_file",methods =['GET','POST'])
def download_file():
    if request.method == 'POST':
        
        return redirect(url_for('home'))
    return render_template("download.html")


@app.route('/azure_pdf_upload', methods=['GET', 'POST'])
def azure_pdf_upload():
    if request.method == 'POST':

        if 'files[]' not in request.files:
            return redirect(request.url)

        files = request.files.getlist('files[]')
        print("The files are: ",files)

        for file in files:
            filename = secure_filename(file.filename)
            
              # make  unique to that point into time 
            dt = datetime.now()

            # make filename unique to that point into time 
            ts = dt.strftime("%Y%m%d_%H%M%S")

            # paste onto file name to make it unique
            time_stamped_filename = f'{ts}_{filename}'
            print("Time stamped file name is: ",time_stamped_filename)
            
            # save the file locally so we can send it to the blob 
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], time_stamped_filename))

            # # Pick container name 
            container_name = "form-recognizer/forms"
            print("Container name is :",container_name)

            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=time_stamped_filename)
            print("The blob client is: ",blob_client)

            upload_file_path = os.path.join(app.config['UPLOAD_FOLDER'], time_stamped_filename)

            print("\nUploading to Azure Storage as blob:\n\t" + time_stamped_filename)

            # Upload the created file
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)

            # Remove file after saving it locally we dont want to have it locally 
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], time_stamped_filename))

        return render_template("upload.html")
    return render_template("upload.html")

@app.route('/listblobs')
def new():
    try:
        container_client2 = blob_service_client2.get_container_client(container=container_name2) # get container client to interact with the container in which images will be stored
        container_client2.get_container_properties()
        print("Container Client Properties are: ",container_client2.get_container_properties())

        print("Listing Blobs")
        blob_items = container_client2.list_blobs()
        print("Blob items are: ",blob_items)

        bloblist =[]
        blobnames = [] 
        res = []
        for blob in blob_items:
            if 'forms/' not in blob.name: # do this because in container the first folder is forms the first file is forms/file1 example 
                print("Wrong subfolder in form-recognizer container! Pass!")
                continue
            else:
                blobname =blob.name
                blob_name = blobname.replace('forms/','')
                blob_client = container_client2.get_blob_client(blob=blob.name) 
                print("The url is ",blob_client.url)
                bloblist.append(blob_client.url)
                blobnames.append(blob_name)

                res1 = {blob_name:blob_client.url} # dict needs to be in list 
                res.append(res1)
                print("DICT IS ",res1)
                
                print(res1)
                
    except Exception as e:
        print(e)
        print("Creating container...")
        container_client2 = blob_service_client2.create_container(container_name2) # create a container in the storage account if it does not exist

    return render_template('listblobs.html',data=bloblist,res = res)

if __name__ == "__main__":
    app.run(debug=True)