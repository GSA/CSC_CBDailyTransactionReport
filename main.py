import re

import pandas as pd
import extn_utils as extn
import send_emails_smtp as se
from datetime import date, timedelta
from googleapiclient.errors import HttpError
import common as c
import base64
filenameList=[];
today = date.today()
#today = today - timedelta(days=2)
print(today)
query ="after: {} from: {} subject: {}".format(today.strftime('%Y/%m/%d'),'No-Reply-OMS@fas.gsa.gov','Report: Automated Camp Butler Daily Transactions Report')
print(query)


def getAttachmentFromInbox():
    try:
        service = c.gmail_authenticate()
        messages = c.search_messages(service, query)
        print(messages)

        for message in messages:
            result = service.users().messages().get(userId='me', id=message['id']).execute()
            emailParts = result['payload']['parts']
            for parts in emailParts:
                filename = parts['filename']
                if filename:
                    attn_id = parts['body']['attachmentId']
                    getAttachment = service.users().messages().attachments().get(userId='me', messageId=message['id'],
                                                                                 id=attn_id).execute()
                    data = getAttachment['data']
                    file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                    attachFolder = f'./CognosReport/{parts["filename"]}'
                    with open(attachFolder, 'wb') as f:
                        f.write(file_data)
                    print('Attachment saved:', filename)
                    filenameList.append(attachFolder)
        return filenameList
    except HttpError as error:
        print(f'An error occurred: {error}')


def executequery(filtered_df):
    allitemno = "('" + "'),('".join(filtered_df['Part Number']) + "')"
    sqlquery = "WITH cte AS (SELECT distinct [Part Number] FROM (VALUES" + allitemno + ") AS T([Part Number])) SELECT c.*, PICS_VENDORPARTNO as VendorPartno FROM cte c left join PICS_CATALOG p  on p.[4PLPARTNO] = c.[Part Number]"
    print(sqlquery);
    return extn.executequery(sqlquery);

def sendemail():
    finalBody = '<p>This report reflects daily transactions for the referenced store and date.</p><p>For questions please contact Vanessa Winter (vanessa.winter@gsa.gov).</p><p>For additional metrics, please visit: https://d2d.gsa.gov/report/grsc-enterprise-4pl-solutions</p>'
    subject = f'Report: Automated Camp Butler Daily Transactions Report-{fileDate}'
    filename = f'Automated Camp Butler Daily Transactions Report-{fileDate}.xlsx'
    #emailAddress = 'shristi.amatya@gsa.gov'
    emailAddress = 'josephe.brown@gsa.gov'
    #allCCEmailAddress = ''
    allCCEmailAddress = 'masami.nagahama.ja@usmc.mil,junko.takahashi@gsa.gov,yoshihiro.aikawa@gsa.gov,tsuyoshi.furugen@gsa.gov,DSSC.GSA.MCBB.FCT@usmc.mil,maria.abrecea@gsa.gov,vanessa.winter@gsa.gov,masami.manna@gsa.gov,evelyn.seiler@gsa.gov,brandy.untalan@gsa.gov,katelyn.young@gsa.gov,shristi.amatya@gsa.gov'
    fromEmail = 'vanessa.winter@gsa.gov'
    #fromEmail = 'shristi.amatya@gsa.gov'
    allBCCEmailAddress = ''
    try:
       #extn.setColumnWidthDynamically(attachment)
       email_params_list = [se.EmailParams(fromEmail, emailAddress, allCCEmailAddress, allBCCEmailAddress, fromEmail, subject, finalBody, [attachment], filename)]
       se.send_email_with_starttls(email_params_list)
    except Exception as e:
        extn.print_colored("An error occurred while sending the email:" + str(e), "red")

def getFileDate(file):
    match = re.search(r"\d{4}-\d{2}-\d{2}",file)
    if match:
        dateStr = match.group()
        print(dateStr)
    else:
        print("no date found")
    return dateStr

if __name__ == '__main__':
   filenameList = getAttachmentFromInbox()
   for file in filenameList:
        df = pd.read_excel(file)
        sqloutput_df = executequery(df);
        merged_df = pd.merge(df, sqloutput_df, on='Part Number', how='left');
        fileDate = getFileDate(file)
        attachment = f'output/Automated Camp Butler Daily Transactions Report-{fileDate}.xlsx';
        merged_df.to_excel(attachment,index=False)
        sendemail();


