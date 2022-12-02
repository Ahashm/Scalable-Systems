var express = require("express");
var router = express.Router();
var dialogflowfulfillment = require("dialogflow-fulfillment");

/* GET users listing. */
router.post("/", express.json(), function (req, res) {
  const agent = new dialogflowfulfillment.WebhookClient({
    request: req,
    response: res,
  });

  function test(agent) {
    agent.add("Stormzy fra Webhook server");
  }

  var intentMap = new Map();

  intentMap.set("TestDemo", test);
  agent.handleRequest(intentMap);
});

/* GET home page. */
router.get("/", function (req, res, next) {
  res.render("index", { title: "Express" });
});

module.exports = router;
