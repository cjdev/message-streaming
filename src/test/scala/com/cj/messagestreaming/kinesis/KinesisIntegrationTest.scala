package com.cj.messagestreaming.kinesis

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.cj.messagestreaming.Publication
import com.cj.tags.IntegrationTest
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}


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


  before(ensureNoDynamoTable())
  after(ensureNoDynamoTable())

  def ensureNoDynamoTable(): Unit = {
    val client = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey))
    client.setRegion(Region.getRegion(Regions.fromName(region)))
    val dynamoDB: DynamoDB = new DynamoDB(client)

    Try(dynamoDB.getTable(applicationName).delete()) match {
      case Success(_) => println("Deleted existing dynamo table")
      case Failure(_) =>
    }
  }


  "The producer and consumer" should "be able to send and receive data" taggedAs IntegrationTest in {

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


    val consumer = makeSubscription(consumerConfig, r => r.getData.array)

    val f: Array[Byte] => Unit = x => {
      recievedRecords.enqueue(x)
      numRecieved += 1
      print("Got one. ")
      if (numRecieved >= recordsToSend.length) {
        gotAllRecords.complete(Success())
      }
    }

    val consumerThread = Future {
      consumer.mapWithCheckpointing(f)
    }


    val config: KinesisProducerConfig = KinesisProducerConfig(
      accessKeyId = awsAccessKeyId,
      secretKey = awsSecretAccessKey,
      region = region,
      streamName = stream
    )


    val pub: Publication[Array[Byte], PublishResult] = makePublication(config, x => x)

    Thread.sleep(30000)

    val futures = recordsToSend.map(pub(_))

    print("Sending records")
    futures.foreach(x => {
      Try {
        Await.result(x, Duration(30, SECONDS))
      } match {
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
        def string(x: Array[Byte]): String = new String(x, "UTF-8")

        recievedRecords.toList.map(string).sorted should be(recordsToSend.map(string).sorted)
      case Failure(e) =>
        e match {
          case _: TimeoutException => fail("Did not get all the records after 30 seconds")
          case _ => fail(e)
        }
    }
  }
}

