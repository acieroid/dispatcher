(ns dispatcher.core
  (:import [org.zeromq ZMQ]))

;; Helper functions
(defn pop-n
  "Call ``pop`` n times on ``coll``"
  [coll n]
  (loop [i 0
         coll coll]
    (if (= i n)
      coll
      (recur (inc i) (pop coll)))))

;; TODO:
;;   - add a callback that will be called when an event is recognized?

;; The dispatcher sends event to a destination (a string representing
;; the URI of a ZeroMQ socket) as map like {:type :event :event x}. It
;; can also send other messages such as {:type :quit}.
(defrecord Dispatcher
    [;; URI where the dispatcher will send the events
     dest-out
     ;; URI from where the dispatcher will read the recognized events
     dest-in
     ;; Maximum throughput (ev/s). Above something like 10k events, it
     ;; seems that some events will be dropped if they are not read
     ;; fast enough at the other end of the socket.
     throughput
     ;; Map of expected events (keys), valued on the timestamp at
     ;; which they were expected
     expected
     ;; Buffer of events to send (a persistent queue)
     buffer])

(defn now
  "Return the current time in milliseconds"
  []
  (System/currentTimeMillis))

(defn- spawn-loop
  "Spawn a background task that sends the events received by the
  dispatcher to its destination."
  [dispatcher]
  (let [ ;; Delay between each sent event
        t (long (/ 1000000000 (:throughput dispatcher)))
        ;; Sleep time between loops (in ns). Max throughput is thus
        ;; limited to 1e9/wait ev/s (10k ev/s here)
        wait 1
        stop (atom false)
        n (atom 0)
        context (ZMQ/context 1)
        socket-out (.socket context ZMQ/PUB)
        socket-in (.socket context ZMQ/SUB)
        _ (.bind socket-out (:dest-out dispatcher))
        _ (.connect socket-in (:dest-in dispatcher))
        _ (.subscribe socket-in (.getBytes "")) ; subscribe to all messages
        ;; wait a bit before publishing events, to avoid losing some
        ;; events
        _ (Thread/sleep 1000)
        buffer (:buffer dispatcher)
        start-time (now)
        thread
        (future
          (try
            (loop [last-send 0]
              (if (> (System/nanoTime) (+ last-send t))
                (do
                  (when (= (mod @n 100) 0)
                    (print (str "\r" @n " messages sent"))
                    (flush))
                  ;; Remember if we need to stop when all remaining
                  ;; expected events are recognized
                  (when (and (not (empty? @buffer))
                             (= (:type (first @buffer)) :stop))
                    (swap! stop (fn [_] true))
                    (swap! buffer pop))
                  ;; Send the events waiting to be sent
                  (when (and (not (empty? @buffer))
                             (= (:type (first @buffer)) :event))
                    (.send socket-out (str (first @buffer)))
                    (swap! n inc)
                    (swap! buffer pop))
                  ;; Handle the expected events stored in the buffer
                  (let [expected (take-while #(= (:type %) :expected) @buffer)]
                    (mapv #(swap! (:expected dispatcher) conj {(:event %)
                                                               (now)})
                          expected)
                    (swap! buffer pop-n (count expected)))
                  ;; Receive the events
                  (when-let [reply (.recv socket-in ZMQ/NOBLOCK)]
                    (let [msg (read-string (String. reply))]
                      (case (keyword (:type msg))
                        :recognition
                        (swap! (:expected dispatcher)
                               #(if-let [expected-t (get % (:event msg))]
                                  (let [time-took (- (now) expected-t)]
                                    (println (str "Recognition: " (:event msg)
                                                  " (took " time-took "ms)"))
                                    (dissoc % (:event msg)))
                                  (do
                                    (println "Unexpected recognition:" (:event msg))
                                    %)))
                        (println "Unexpected reply:" msg))))
                  (if (and @stop
                           (empty? @(:expected dispatcher)))
                    (let [duration (float (/ (- (now) start-time) 1000))]
                      (println "\nSent" @n "events in " duration "seconds"
                               (str "(" (long (/ @n duration)) " events/s)"))
                      (.send socket-out (str {:type :quit}))
                      (.close socket-out)
                      (.close socket-in)
                      (.term context)
                      (shutdown-agents))
                    (do
                      (when @stop
                        ;; Just  wait for the recognitions to happen
                        (Thread/sleep 1000))
                      (recur (System/nanoTime)))))
                (do
                  ;; It seems that calling Thread/sleep takes around
                  ;; at least 1ms, thus limiting the throughput to at
                  ;; most 1000 ev/s.
                  ;; (Thread/sleep 0 wait)
                  (recur last-send))))
            (catch Exception e
              (println "Error in background task:" e)
              (.printStackTrace e))))]
    (update-in dispatcher [:thread] (fn [_] thread))))

(defn create
  "Create a dispatcher and launch a background task that will send the
  messages without exceeding the given throughput."
  [dest-in dest-out throughput]
  (let [dispatcher (Dispatcher. dest-in dest-out throughput
                                (atom {})
                                (atom clojure.lang.PersistentQueue/EMPTY))]
    ;; Spawn loop updates the dispatcher and returns it (and creates
    ;; the background loop that sends the events)
    (spawn-loop dispatcher)))

(defn enqueue
  "Enqueue an event to be sent later"
  [dispatcher event]
  (swap! (:buffer dispatcher) #(conj % {:type :event :event event}))
  dispatcher)

(defn expect
  "Remembers that an event is expected to happen now. Will compute the
  delay between now and the time it will really happen"
  [dispatcher event]
  (swap! (:buffer dispatcher) conj {:type :expected :event event})
  dispatcher)

(defn cancel
  "Immediately stops the dispatcher"
  [dispatcher]
  (future-cancel (:thread dispatcher))
  dispatcher)

(defn finish
  "Don't send more events than those already stored in the
  buffer. Waits for all expected events, then return."
  [dispatcher]
  (swap! (:buffer dispatcher) conj {:type :stop})
  dispatcher)

(defn abort
  "Directly send a stop message and drop all bufferised events"
  [dispatcher]
  (swap! (:buffer dispatcher) (fn [b] [{:type :stop}]))
  dispatcher)
