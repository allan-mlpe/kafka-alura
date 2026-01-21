classDiagram
direction BT
class ConsumerService~T~ {
<<Interface>>
  + parse(ConsumerRecord~String, Message~T~~) void
   String consumerGroup
   String topic
}
class EmailService {
  + EmailService() 
  + parse(ConsumerRecord~String, Message~Email~~) void
  + main(String[]) void
   String consumerGroup
   String topic
}
class ReadingReportService {
  + ReadingReportService() 
  + parse(ConsumerRecord~String, Message~User~~) void
  + main(String[]) void
   String consumerGroup
   String topic
}
class ServiceFactory~T~ {
<<Interface>>
  + create() ConsumerService~T~
}
class ServiceProvider~T~ {
  + ServiceProvider(ServiceFactory~T~) 
  + call() Void
}
class ServiceRunner~T~ {
  + ServiceRunner(ServiceFactory~T~) 
  + start(int) void
}

EmailService  ..>  ConsumerService~T~ 
EmailService  ..>  ServiceRunner~T~ : «create»
ReadingReportService  ..>  ConsumerService~T~ 
ReadingReportService  ..>  ServiceRunner~T~ : «create»
ServiceProvider~T~ "1" *--> "factory 1" ServiceFactory~T~ 
ServiceRunner~T~ "1" *--> "provider 1" ServiceProvider~T~ 
ServiceRunner~T~  ..>  ServiceProvider~T~ : «create»
