const HOST = "http://localhost:5000/";

var map = L.map("mapid").setView([51.9189046, 19.1343786], 6);
function AddTiles(map) {
  var Stadia_AlidadeSmooth = L.tileLayer(
    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    {
      maxZoom: 20,
      attribution:
        '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors',
    }
  ).addTo(map);
}

AddTiles(map);

async function getOffersData() {
  let response = await fetch(HOST + "offers/", { mode: "no-cors" });
  let data = await response.json();
  console.log(data);
  return data;
}

getOffersData()
  .then((data) => console.log(data))
  .catch((reason) => console.log(reason.message));

var marker = L.marker([51.5, -0.09]).addTo(map);
