(defproject fourtytoo/francesco "0.1.0-SNAPSHOT"
  :description "An async library wrapper for Franzy (the Kafka Clojure library)."
  :url "http://github.com/fourtytoo/francesco"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.4.474"]
                 ;; [cprop "0.1.17"]
                 [org.clojure/tools.logging "1.1.0"]
                 [clj-kafka.franzy/core "2.0.7"]
                 [clj-kafka.franzy/common "2.0.7"]
                 [clj-kafka.franzy/admin "2.0.7"]])
