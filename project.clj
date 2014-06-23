(defproject dispatcher "0.0.1-SNAPSHOT"
  :description "Send events to a zmq server with a given throughput"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [naughtmq "0.0.2"]
                 [org.zeromq/jzmq "2.2.2"]]
  :repositories {"sonatype-nexus-snapshots" "https://oss.sonatype.org/content/repositories/snapshots"})
