package whu.edu.cn

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import geotrellis.layer.SpatialKey
import geotrellis.raster._
import geotrellis.raster.render.{ColorMap, Exact, Png}
import geotrellis.store.{AttributeStore, LayerId, Reader, ValueNotFoundError, ValueReader}

import scala.concurrent._

object Serve {
  def main(args: Array[String]): Unit = {
    val outputPath = "Path of data" // for example: "/Users/tankenqi/Downloads/data"
    val catalogPath = new java.io.File(outputPath).toURI
    val attributeStore: AttributeStore = AttributeStore(catalogPath)
    val valueReader: ValueReader[LayerId] = ValueReader(attributeStore, catalogPath)

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val route: Route = cors() {
      pathPrefix(Segment) {
        layerId =>
          pathPrefix(IntNumber) {
            zoom =>
              // ZXY route:
              pathPrefix(IntNumber / IntNumber) { (x, y) =>
                complete {
                  Future {
                    // Read in the tile at the given z/x/y coordinates.
                    val tileOpt: Option[MultibandTile] =
                      try {
                        val reader: Reader[SpatialKey, MultibandTile] = valueReader.reader[SpatialKey, MultibandTile](LayerId(layerId, zoom))
                        Some(reader.read(x, y))
                      } catch {
                        case _: ValueNotFoundError =>
                          None
                      }

                    var png: Png = null
                    for (tile <- tileOpt) yield {
                      val product: MultibandTile = rasterFunction(tile)
                      val bandCount: Int = product.bandCount
                      if (bandCount == 1) {
                        png = product.band(0).renderPng()
                      }
                      else if (bandCount == 2) {
                        png = MultibandTile(product.bands.take(2)).renderPng()
                      }
                      else if (bandCount == 3) {
                        png = MultibandTile(product.bands.take(3)).renderPng()
                      }
                      else {
                        throw new RuntimeException("波段数量不是1或3，无法渲染！")
                      }

                    }
                    pngAsHttpResponse(png)

                  }
                }
              }
          }
      } ~
        // Static content routes:
        pathEndOrSingleSlash {
          getFromFile("static/index.html")
        } ~
        pathPrefix("") {
          getFromDirectory("static")
        }
    }
    val binding: Future[Http.ServerBinding] = Http().bindAndHandle(route, "0.0.0.0", 19100)
  }

  /** raster transformation to perform at request time */
  def rasterFunction(multibandTile: MultibandTile): MultibandTile = {
    multibandTile.convert(DoubleConstantNoDataCellType)
  }

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))
}
