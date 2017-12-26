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

              // Make logs to pass to Avro
/*              def jsonbody = new JsonSlurper().parseText(message.body)
              def json = new JsonSlurper().parseText(jsonbody.Message)
              sqs_logs = new JsonBuilder(ingestdate: ingestdate, starttime: starttime, acquistiondate: json.observationDateTime,
                      imageId: json.imageId, url: json.uRL)

              message.body["sqs_logs"] = sqs_logs */

              switch(destinationType)
              {
                case "stdout":
                  messagesToDelete << message
                  break
                case "post":
                  url = config.reader.destination.post.urlEndPoint
                  println "message.body" + message.body
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

            def jsonbody = new JsonSlurper().parseText(message.body)
            def json = new JsonSlurper().parseText(jsonbody.Message)
            sqs_logs = new JsonBuilder(ingestdate: ingestdate, procTime: procTime, acquistiondate: json.observationDateTime,
            imageId: json.imageId, url: json.uRL)

//            log.info sqs_logs.toString()
            // Printing to avoid log header.
            println sqs_logs.toString()

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
