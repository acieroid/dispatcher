(defproject dispatcher "0.0.1-SNAPSHOT"
  :description "Send events to a zmq server with a given throughput"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org/jeromq/jeromq "0.3.0-SNAPSHOT"]
                 [cheshire "5.2.0"]]
  :repositories {"sonatype-nexus-snapshots" "https://oss.sonatype.org/content/repositories/snapshots"})
