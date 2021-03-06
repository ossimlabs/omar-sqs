package sqs.app

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper


class SqsReaderJob {
   def sqsService
   def concurrent = false

   static triggers = {
      simple repeatInterval: 5000l, name: 'SqsReaderTrigger', group: 'SqsReaderGroup'
   }

  def getLogMessage() {

  }

  def execute() {
    Boolean keepGoing = true
    def messages
    def config = SqsUtils.sqsConfig
    def ingestdate
    def destinationType = config.reader.destination.type.toLowerCase()
    if(config.reader.queue)
    {
      while(messages = sqsService?.receiveMessages())
      {
        ingestdate = new Date().format("yyyy-MM-dd HH:mm:ss.SSS")

        def messagesToDelete = []
        def messageBodyList  = []
        String url
        messages?.each{message->
          try{
            if(sqsService.checkMd5(message.mD5OfBody, message.body))
            {
              switch(destinationType)
              {
                case "stdout":
                  messagesToDelete << message
                  break
                case "post":
                  url = config.reader.destination.post.urlEndPoint

                  def jsonbody = new JsonSlurper().parseText(message.body)
                  def json = new JsonSlurper().parseText(jsonbody.Message)

                  json["ingest_date"] = ingestdate
                  json["acquisition_date"] = json.observationDateTime
                  json["image_id"] = json.imageId
                  json["url"] = json.uRL

                  jsonbody.Message = new JsonBuilder(json).toString()
                  message.body = new JsonBuilder(jsonbody)


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
