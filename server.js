const express = require('express')
const app = express()
const port = 3432

app.get('/', (req, res) => {
	res.sendFile(__dirname + "/all.html")
})

app.listen(port, () => {
	console.log(`IsoDiff GUI Server @ http://localhost:${port}`)
})
