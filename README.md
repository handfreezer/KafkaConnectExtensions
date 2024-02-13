# KafkaConnectExtensions
## Description
You will find here few lines of code to extend Kafka Connect Framework.
It is for few work-in-progress or work-in-test or final-code .

Here is a simple status:

| Directory | Module | Status | Description |
| ------------ | ------------ | ------------ | ------------ |
| mirror | RegexReplicationPolicy | **final-code** | It is a replication policy for MM2 ( Mirror Maker 2 or MirrorMaker2 ) which allow you to transform the topic name on replication side as you want by regex with extraction (if topic is named toto-tata , a regex like /toto-(.*)/ and a replacement by titi-$1 will result to titi-tata as name of replicated topic). |
| smt |  AddUuid | **final-code** | It is a SMT for Kafka Connect (also known as a Simple Message Transform or SimpleMessageTranform) which has for only goal to add a header with a user-defined fieldname and a random UUID. |
| smt | AvroToJson | *work-in-test* | As the name of the module is saying : the goal is to convert a AVRO message formatted to a JSON string according to Schema stored in a registry. The code was AI helped and never tested (and will probably never) ! |

## How-to use
It is very simple:
- for mirror modules : as replication policy are not managed as plugin by Kafka Connect, the code have to be inserted in JVM as a library, so just compile with `mvn package` and place it in your 
- for smt modules : as SMT are managed as plugins by KafkaConnect, the code have to be declared in the plugin.path parameter of your Kafka Connect Node, so just compile with `mvn package` and place ir in a directory pointed by plugin.path parameter.

To have a demo of this, you can consider cloning [KafkaPoc-MM2-And-Design-SMT](https://github.com/handfreezer/KafkaPoc-MM2-And-Design-SMT "KafkaPoc-MM2-And-Design-SMT") which is using the stuff here.