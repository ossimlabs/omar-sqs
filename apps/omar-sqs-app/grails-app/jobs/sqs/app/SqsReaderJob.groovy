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
    def destinationType = config.reader.destination.type.toLowerCase()
    if(config.reader.queue)
    {
      log.debug "Testing for SQS messages"
      while(messages = sqsService?.receiveMessages())
      {
        log.debug "TRAVERSING MESSAGES"

        ingestdate = new Date().format("YYYY-MM-DD HH:mm:ss.Ms")

        log.info "Ingested an image at time: " + ingestdate


        def messagesToDelete = []
        def messageBodyList  = []
        messages?.each{message->
          try{
            starttime = System.currentTimeMillis()
            log.debug "Checking Md5 checksum"
            if(sqsService.checkMd5(message.mD5OfBody, message.body))
            {
              log.debug "PASSED MD5"

              // try a output here and if fails then do not mark the message 
              // for deletion
              //
              switch(destinationType)
              {
                case "stdout":
                  log.info message.body
                  messagesToDelete << message
                  break
                case "post":
                  String url = config.reader.destination.post.urlEndPoint
                  log.info "Posting message to ${url}"
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
        log.info "MESSAGES DELETING!!!!"
        if(messagesToDelete) sqsService.deleteMessages(
                                       SqsUtils.sqsConfig.reader.queue,
                                       messagesToDelete)
        messagesToDelete = []

        endtime = System.currentTimeMillis()
        procTime = endtime - starttime
        log.info "time for ingest: " + procTime

        sqs_logs = new JsonBuilder(ingestdate: ingestdate, procTime: procTime, inboxurl: url)

        log.info sqs_logs.toString()

      }
    }
  }
}
