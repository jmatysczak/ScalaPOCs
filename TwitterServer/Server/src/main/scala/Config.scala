package jmat.scalapocs.server

import com.twitter.app.GlobalFlag

package config {

  object responseName extends GlobalFlag[String]("World", "The name to respond with.")
}
