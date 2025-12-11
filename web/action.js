const ui_btn_run = document.getElementById('btn_run');
const ui_txt_resp = document.getElementById('response');
const ui_input_args = document.getElementById('inputData');
const ui_select_cmd = document.getElementById('mySelect');

const socket_hello = new WebSocket(SERVER);
socket_hello.onmessage = function(event) {
	console.log(`Server response to HELLO: ${event.data}`);
	let resp_hi = JSON.parse(event.data);
	updateSelect(resp_hi);
};
socket_hello.onopen = function(event) {
	console.log('WS_HELLO Connection OK :) Say hello~');
	socket_hello.send(JSON.stringify({'type': 'hello'}));
};
socket_hello.onclose = function(event) {
	console.log('WS_HELLO Connection CLOSED.');
};
function updateSelect(options) {
    ui_select_cmd.innerHTML = "";
    options.forEach(item => {
        const option = document.createElement("option");
        option.value = item;
        option.textContent = item;
        ui_select_cmd.appendChild(option);
    });
}

function sendData() {
	let resp = null;
	let socket_c = null;
	ui_btn_run.disabled = true;
	ui_txt_resp.innerText += 'Connecting to server...\n';
	socket_c = new WebSocket(SERVER);
	socket_c.onmessage = function(event) {
		resp = event.data;
		ui_txt_resp.value += resp + "\n";
	};	
	socket_c.onopen = function(event) {
		console.log('WS_C Connection OK :)');
		ui_txt_resp.value = "Server connected.\n";
		const value = ui_input_args.value;
		const payload = {
			data: value,
			type: "cmd",
			param: ui_select_cmd.value
		};
		socket_c.send(JSON.stringify(payload));
	};
	socket_c.onclose = function(event) {
		console.log('WS_C Connection CLOSED.');
	};
}