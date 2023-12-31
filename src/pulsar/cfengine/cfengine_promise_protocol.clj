(ns pulsar.cfengine.cfengine-promise-protocol
  (:require [cheshire.core :as cheshire]
            [clojure.spec.alpha :as spec]
            [clojure.string :as string]))
;;https://docs.cfengine.com/docs/3.22/reference-promise-types-custom.html

(def PROMISE-KEPT "kept")
(def PROMISE-REPAIRED "repaired")
(def PROMISE-NOT-KEPT "not_kept")
(def PROMISE-SUCCESS "success")
(def PROMISE-FAILURE "failure")
(def PROMISE-ERROR "error")
(def PROMISE-VALID "valid")
(def PROMISE-INVALID "invalid")

(defn clean-log-message [msg]
  (string/replace msg "\n" ""))

;;https://ask.clojure.org/index.php/11449/macroexpanding-macro-with-crashes-with-did-not-conform-spec
;;(macroexpand-1 '(create-promise "kept" "warn"))
(defmacro create-promise [promise-type log-level-default]
  (let [pname# (symbol (str "promise-" promise-type))
        ptype# (symbol (str "PROMISE-" (string/upper-case promise-type)))]
    `(defn ~pname#
       ([~'log] (~pname# ~'log ~log-level-default))
       ([~'log ~'log-level]
        {:result ~ptype# :log [{:level ~'log-level :message (clean-log-message ~'log)}]}))))

(create-promise "kept" "info")
(create-promise "repaired" "info")
(create-promise "not-kept" "error")
(create-promise "valid" "info")
(create-promise "invalid" "error")
(create-promise "success" "info")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;Parse raw input lines (strings)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti parse-raw-line
  (fn [input-line]
    (if (string/starts-with? input-line "cf-agent")
      ::cf-agent-header
      ::cf-operation)))

(defmethod parse-raw-line ::cf-agent-header [line]
  {:operation ::header :header-line line})

(defmethod parse-raw-line ::cf-operation [line]
  (cheshire/parse-string line true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;Handle Operations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def carry-foward-attr-keys
  "keys in the cf-engine input that should be carried to the output"
  [:operation :promiser :attributes])
  
(defn enrich-output [cfe-input module-output]
  (conj (select-keys cfe-input carry-foward-attr-keys)
        module-output))

(defn op-dispatch-fn [ctx {operation :operation}] operation) ;;allow repl reloading
(defmulti handle-operation #'op-dispatch-fn)

(defmethod handle-operation ::header [{module-name ::module-name
                                      version-number ::version-number}
                                      {header-line :header-line}]
  (if-not (string/ends-with? header-line "v1")
    (throw (Exception. "This library only supports v1 of the cfengine custom promise protocol")))
  {:header-line (str module-name " " version-number " v1 json_based")})

(defmethod handle-operation "validate_promise" [{promiser-spec ::promiser-spec
                                                 promise-attrs-spec ::promise-attrs-spec}
                                                {promiser :promiser
                                                 attributes :attributes}]
  (let [explain-promiser (spec/explain-str promiser-spec promiser)
        explain-attributes (spec/explain-str promise-attrs-spec attributes)]
    (if (not (string/starts-with? explain-promiser "Success"))
      (promise-invalid explain-promiser)
      (if (not (string/starts-with? explain-attributes "Success"))
        (promise-invalid explain-attributes)
        (promise-valid "Promise validated Successfully")))))

(defmethod handle-operation "evaluate_promise" [{evaluation-fn ::evaluation-fn}
                                                {promiser :promiser
                                                 attributes :attributes}]
  (evaluation-fn promiser attributes))
        
(defmethod handle-operation "terminate" [{module-name ::module-name} _ ]
  (promise-success (str module-name " completed successfully")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Entry Point
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn start-promise-module [^java.io.BufferedReader input-reader
                            module-name
                            version-number
                            promiser-spec
                            promise-attrs-spec
                            evaluation-fn]
  "input-reader (java.io.reader) : to read from, should likely wrap *in* unless testing
   module-name (string) : The name of the custom promise module
   version-number (string) : a version number for the module
   promiser-spec (spec) : a clojure spec that verifies the promiser input
   promise-attrs-spec (spec) : a clojure spec that verifies the promise attributes
   evaluation-fn (fn) : A function that evaluates the promise, triggered by the 'evaluate_operation' operation, it should use a helper return function i.e. 'promise-success' etc
  "
  (let [ctx {::module-name module-name
             ::version-number version-number
             ::promiser-spec promiser-spec
             ::promise-attrs-spec promise-attrs-spec
             ::evaluation-fn evaluation-fn}
        op-handler (partial handle-operation ctx)]
    (loop [line (.readLine input-reader)]
      (if line
        (if (not= line "")
          (let [cfe-input (parse-raw-line line)
                op-output (op-handler cfe-input)
                enriched (enrich-output cfe-input op-output)
                operation (:operation enriched)]
            (if (= operation ::header)
              (println (:header-line enriched) "\n")
              (println (cheshire/generate-string enriched) "\n"))
            (if (not= operation "terminate")
              (recur (.readLine input-reader))))
          (recur (.readLine input-reader))))))
  (.close input-reader))

