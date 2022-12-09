var express = require("express");
var router = express.Router();
var mongoose = require("mongoose");
mongoose.connect("mongodb://localhost:27017/test", (err) => {
  if (!err) {
    console.log("connected to db");
  } else {
    console.log("error");
  }
});
var Schema = mongoose.Schema;

var userDataSchema = new Schema(
  {
    name: String,
  },
  { collection: "user-data" }
);

var UserData = mongoose.model("UserData", userDataSchema);

/* GET home page. */
router.get("/", function (req, res, next) {
  res.render("index", { title: "Express" });
});

router.get("/get-data", function (req, res, next) {
  UserData.find().then(function (doc) {
    res.render("index", { items: doc });
  });
});

router.post("/insert", function (req, res, next) {
  var item = {
    name: req.body.name,
  };

  var data = new UserData(item);
  data.save();

  res.redirect("/");
});

module.exports = router;
