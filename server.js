const express = require("express")
const d3 = require("d3")
const app = express()
const port = 3432

const parser = require("./parser.js")
const vis = require("./vis.js")

app.set('view engine', 'ejs');

app.get("/", (req, res) => {
	let svgOfGraph = vis.drawGraph(parser.default_test)
	console.log(svgOfGraph.svgString())
	res.render("index", {svgGraph: svgOfGraph.svgString()})
})

app.listen(port, () => {
	console.log("IsoDiff GUI Server @ http://localhost:${port}")
})

