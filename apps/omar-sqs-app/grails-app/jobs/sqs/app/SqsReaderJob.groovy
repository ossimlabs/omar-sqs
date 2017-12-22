package sqs.app

import groovy.json.JsonBuilder


class SqsReaderJob {
   def sqsService

   static triggers = {
      simple name: 'SqsReaderTrigger', group: 'SqsReaderGroup'
   }

  def execute() {
    Boolean keepGoing = true
    def messages
    def config = SqsUtils.sqsConfig
    def starttime
    def endtime
    def procTime
    def ingestdate
    def sqs_logs
    def destinationType = config.reader.destination.type.toLowerCase()
    if(config.reader.queue)
    {
      while(messages = sqsService?.receiveMessages())
      {
        ingestdate = new Date().format("yyyy-MM-dd hh:mm:ss.ms")

        def messagesToDelete = []
        def messageBodyList  = []
        String url
        messages?.each{message->
          try{
            starttime = System.currentTimeMillis()
            if(sqsService.checkMd5(message.mD5OfBody, message.body))
            {
              // try a output here and if fails then do not mark the message
              // for deletion
              //
              switch(destinationType)
              {
                case "stdout":
                  messagesToDelete << message
                  break
                case "post":
                  url = config.reader.destination.post.urlEndPoint
                  def result = sqsService.postMessage(url, message.body)
                 // is a 200 range response
                 //
                  if((result?.status >= 200) && (result?.status <300))
                  {
                    messagesToDelete << message
                  }
                  else
                  {
                    log.error result?.message?.toString()
                  }
                  break
              }
            }
            else
            {
              log.error("ERROR: BAD MD5 Checksum For Message: ${messageBody}")
              messagesToDelete << message
            }

            endtime = System.currentTimeMillis()
            procTime = endtime - starttime
            sqs_logs = new JsonBuilder(ingestdate: ingestdate, procTime: procTime, message: message.body)

            log.info sqs_logs.toString()

          }
          catch(e)
          {
            log.error("ERROR: ${e.toString()}")
          }

          messageBodyList = []
        }
        if(messagesToDelete) sqsService.deleteMessages(
                                       SqsUtils.sqsConfig.reader.queue,
                                       messagesToDelete)
        messagesToDelete = []


      }
    }
  }
}
