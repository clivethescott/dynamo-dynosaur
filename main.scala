//> using scala 2.13.15
//> using dep org.systemfw::dynosaur-core:0.7.0
//> using dep software.amazon.awssdk:dynamodb:2.30.1
//> using dep ch.qos.logback:logback-classic:1.5.16
//> using resourceDir ./resources

import scala.util.Try
import scala.jdk.CollectionConverters._
import scala.collection

import dynosaur.Schema
import cats.syntax.all._
import software.amazon.awssdk.core.internal.http.AmazonSyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  PutItemRequest
}
import DB.dynamoClient
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import dynosaur.DynamoValue

object Model {
  final case class Job(id: Int, name: String)
  final case class Person(id: String, name: String, job: Option[Job])

  implicit class EitherOps[A, E](either: Either[E, A]) {
    def unit: Either[E, Unit] = either.map(_ => ())
  }
}

object DB {
  import Model._

  implicit val jobSchema: Schema[Job] = Schema.record { field =>
    (
      field("id", _.id),
      field("name", _.name)
    ).mapN(Job.apply)
  }

  implicit val personSchema: Schema[Person] = Schema.record { field =>
    (
      field("id", _.id),
      field("name", _.name),
      field.opt("job", _.job)
    ).mapN(Person.apply)
  }

  val dynamoClient: DynamoDbClient = DynamoDbClient
    .builder()
    .endpointOverride(java.net.URI.create("http://localhost:8000"))
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create("xyz", "xyz"))
    )
    .build()
}

object Main {
  import Model._
  def main(args: Array[String]): Unit = {

    val job = Job(id = 5, name = "Mechanic")
    val person = Person(id = "1", name = "John Doe", job = Some(job))

    import DB._ // import implicits
    write(person) match {
      case Left(err) => println(s"failed to write to Dynamo: $err")
      case _         => println("Dynamo OK!")
    }

    val personRead = read[Person](person.id)
    println(s"Read person: $personRead")

  }

  def read[A: Schema](id: String): Either[Throwable, A] =
    for {
      dv <- readItem(id)
      value <- implicitly[Schema[A]].read(dv)
    } yield value

  def write[A: Schema](value: A): Either[Throwable, Unit] =
    for {
      dv <- implicitly[Schema[A]].write(value)
      _ <- putItem(dv)
    } yield ()

  private def putItem(item: DynamoValue): Either[Throwable, Unit] = {
    val putItemRequest = item.attributeMap.map(item =>
      PutItemRequest
        .builder()
        .tableName("ddb-test")
        .item(item)
        .build()
    )

    putItemRequest
      .toRight(new Throwable(s"item value wasn't a map: $item"))
      .flatMap(req => Try(dynamoClient.putItem(req)).toEither.unit)
  }

  private def readItem(id: String): Either[Throwable, DynamoValue] = {
    val pk = DynamoValue.m(("id", DynamoValue.s(id)))
    pk.attributeMap
      .toRight(new Throwable(s"Item not a map: $pk"))
      .map(dv =>
        GetItemRequest
          .builder()
          .tableName("ddb-test")
          .attributesToGet("id", "name", "job")
          .key(dv)
          .build()
      )
      .flatMap(req => Try(dynamoClient.getItem(req)).toEither)
      .flatMap(response =>
        Either.cond(
          response.hasItem(),
          DynamoValue.attributeMap(response.item()),
          new Throwable("Item could not be found")
        )
      )
  }
}
