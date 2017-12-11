import itchat
from itchat.content import TEXT

@itchat.msg_register(TEXT)
def text_reply(msg):
    print(msg)
    itchat.send(msg['Text'], msg['FromUserName'])

itchat.auto_login()
itchat.run()