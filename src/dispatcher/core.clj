(ns dispatcher.core)

(defrecord Dispatcher [destination throughput expected buffer])

(defn create
  "Create a dispatcher and launch a background task that will send the
  messages without exceeding the given throughput."
  [destination throughput]
  (let [dispatcher (Dispatcher.)])
  TODO)

(defn enqueue
  "Enqueue an event to be sent later"
  [dispatcher event]
  TODO)

(defn expect
  "Remembers that an event is expected to happen now. Will compute the
  delay between now and the time it will really happen"
  [dispatcher event]
  TODO)
