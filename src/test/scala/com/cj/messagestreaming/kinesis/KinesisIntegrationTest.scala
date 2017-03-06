package com.cj.messagestreaming.kinesis

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.cj.messagestreaming.{Publication, Subscription}
import com.cj.messagestreaming.kinesis.Kinesis.{KinesisConsumerConfig, KinesisProducerConfig, makePublication, makeSubscription}
import com.cj.tags.IntegrationTest
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.SECONDS
import scala.concurrent.{Await, Future, Promise, TimeoutException}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBClient, document}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Table}


class KinesisIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  def envOrFail(e: String): String = {
    Option(System.getenv(e)).getOrElse(
      fail(s"Missing environment varialble: $e")
    )
  }

  lazy val awsAccessKeyId = envOrFail("AWS_ACCESS_KEY_ID")
  lazy val awsSecretAccessKey = envOrFail("AWS_SECRET_ACCESS_KEY")
  val region: String = "us-west-1"
  val stream: String = "testing"
  val applicationName = "test"
  val workerId = "test"


  before( ensureNoDynamoTable() )
  after(  ensureNoDynamoTable() )

  def ensureNoDynamoTable(): Unit = {
    val client = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey))
    client.setRegion(Region.getRegion(Regions.fromName(region)))
    val dynamoDB: DynamoDB = new DynamoDB(client)

    Try(dynamoDB.getTable(applicationName).delete()) match {
      case Success(_) => println("Deleted dynamo table")
      case Failure(_) => println("Didn't delete dynamo table")
    }
  }


  "The producer and consumer" should "be able to send and receive data" taggedAs IntegrationTest in {


    println(awsAccessKeyId)
    val recordsToSend: List[Array[Byte]] = 1.to(5).map(_.toString()).map(_.getBytes).toList
    val recievedRecords: mutable.Queue[Array[Byte]] = new mutable.Queue()
    var numRecieved: Int = 0
    val gotAllRecords = Promise[Unit]


    val consumerConfig: KinesisConsumerConfig = KinesisConsumerConfig(
      accessKeyId = awsAccessKeyId,
      secretKey = awsSecretAccessKey,
      region = region,
      streamName = stream,
      applicationName = applicationName,
      workerId = workerId
    )


    val consumer =  makeSubscription(consumerConfig)


    val f: Array[Byte] => Unit = x => {
      recievedRecords.enqueue(x)
      numRecieved += 1
      print("Got one. ")
      if (numRecieved >= recordsToSend.length) {
        gotAllRecords.complete(Success())
      }
    }

    val consumerThread = Future{ consumer.mapWithCheckpointing(f) }


    val config: KinesisProducerConfig = KinesisProducerConfig(
      accessKeyId = awsAccessKeyId,
      secretKey = awsSecretAccessKey,
      region = region,
      streamName = stream
    )


    val pub: Publication = makePublication(config)

    Thread.sleep(30000)

    val futures = recordsToSend.map(pub(_))

    print("Sending records")
    futures.map(x => {
      Try{ x.get(30,SECONDS) } match {
        case Success(_) => print(".")
        case Failure(e) => fail(e)
      }
    })
    println()

    println("Waiting for consumer...")

    val result = Try(Await.result(gotAllRecords.future, Duration(30, SECONDS)))
    println()

    result match {
      case Success(_) =>
        def string(x: Array[Byte]): String = new String(x,"UTF-8")
        recievedRecords.toList.map(string).sorted should be (recordsToSend.map(string).sorted)
      case Failure(e) =>
        e match {
          case _:TimeoutException => fail("Did not get all the records after 30 seconds")
          case _ => fail(e)
        }
    }
  }
}

