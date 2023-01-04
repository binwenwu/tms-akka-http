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
import geotrellis.store.{AttributeStore, LayerId, ValueNotFoundError, ValueReader}

import scala.concurrent._

object Serve {
  def main(args: Array[String]): Unit = {
    val outputPath = "/home/geocube/oge/on-the-fly"
    val catalogPath = new java.io.File(outputPath).toURI
    //创建存储区
    val attributeStore: AttributeStore = AttributeStore(catalogPath)
    //创建valuereader，用来读取每个tile的value
    val valueReader: ValueReader[LayerId] = ValueReader(attributeStore, catalogPath)

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val route: Route = cors() {
      pathPrefix(Segment) {
        layerId =>
          val renderArray = layerId.split("_")(1).split("-")
          val palette = renderArray(0)
          val min = renderArray(1).toInt
          val max = renderArray(2).toInt
          pathPrefix(IntNumber) {
            zoom =>
              val fn: Tile => Tile = Serve.rasterFunction()
              // ZXY route:
              pathPrefix(IntNumber / IntNumber) { (x, y) =>
                complete {
                  Future {
                    // Read in the tile at the given z/x/y coordinates.
                    val tileOpt: Option[Tile] =
                      try {
                        val reader = valueReader.reader[SpatialKey, Tile](LayerId(layerId, zoom))
                        Some(reader.read(x, y))
                      } catch {
                        case _: ValueNotFoundError =>
                          None
                      }
                    for (tile <- tileOpt) yield {
                      val product: Tile = fn(tile)
                      var png: Png = null

                      if (palette == null) {
                        val colorMap = {
                          ColorMap(
                            scala.Predef.Map(
                              max -> geotrellis.raster.render.RGBA(255, 255, 255, 255),
                              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20),
                              -1 -> geotrellis.raster.render.RGBA(255, 255, 255, 255)
                            ),
                            ColorMap.Options(
                              classBoundaryType = Exact,
                              noDataColor = 0x00000000, // transparent
                              fallbackColor = 0x00000000, // transparent
                              strict = false
                            )
                          )
                        }
                        png = product.renderPng(colorMap)
                      }
                      else if ("green".equals(palette)) {
                        val colorMap = {
                          ColorMap(
                            scala.Predef.Map(
                              max -> geotrellis.raster.render.RGBA(127, 255, 170, 255),
                              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20)
                            ),
                            ColorMap.Options(
                              classBoundaryType = Exact,
                              noDataColor = 0x00000000, // transparent
                              fallbackColor = 0x00000000, // transparent
                              strict = false
                            )
                          )
                        }
                        png = product.renderPng(colorMap)
                      }
                      else if("origin".equals(palette)){
                        val colorRamp = ColorRamp(0xFF000000, 0xFFFFFFFF).stops(100)
                        png = product.renderPng(colorRamp)
                      }
                      else {
                        val colorRamp = ColorRamp(
                          0xD76B27FF,
                          0xE68F2DFF,
                          0xF9B737FF,
                          0xF5CF7DFF,
                          0xF0E7BBFF,
                          0xEDECEAFF,
                          0xC8E1E7FF,
                          0xADD8EAFF,
                          0x7FB8D4FF,
                          0x4EA3C8FF,
                          0x2586ABFF
                        )
                        png = product.renderPng(colorRamp)
                      }
                      pngAsHttpResponse(png)
                    }
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
  def rasterFunction(): Tile => Tile = {
    tile: Tile => tile.convert(DoubleConstantNoDataCellType)
  }

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))
}
