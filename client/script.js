const API_URL = "http://localhost:8000/api/v1";

var map = L.map("mapid", { preferCanvas: true }).setView(
  [51.9189046, 19.1343786],
  6
);
var markers = new L.LayerGroup();
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

async function getData(url) {
  let full_url = url;
  const response = await fetch(full_url, {
    mode: "cors",
    headers: {
      "Content-Type": "application/json",
    },
  });
  const data = await response.json();
  return data;
}

async function postData(url = "", data = {}) {
  let full_url = url;
  const response = await fetch(full_url, {
    method: "POST",
    mode: "cors",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
  return response.json();
}

function roundLargeToThousands(number) {
  let rounded =
    Math.abs(number / 10000) > 1
      ? Math.round(number / 1000) + " tys. "
      : Math.round(number);
  return rounded;
}

function getOfferHtml(offer) {
  let price_pred_diff = offer.estimate - offer.price;
  let color_class = price_pred_diff > 0 ? "text-success" : "text-danger";
  html = `
  <table class="table table-hover">
    <tbody>
      <tr>
        <td>Cena:</td>
        <td><strong>${roundLargeToThousands(offer.price)} zł</strong></td>
      </tr>
      <tr>
        <td>Warte:</td>
        <td><strong>${roundLargeToThousands(
          (offer.estimate / 50) * 50
        )} zł</strong></td>
      </tr>
      <tr>
        <td>Różnica:</td>
        <td><strong class="${color_class}">${roundLargeToThousands(
    price_pred_diff
  )} zł </strong></td>
      </tr>
      <tr>
        <td>Powierzchnia:</td>
        <td><strong> ${Math.round(offer.size)}m2 </strong></td>
      </tr>
      <tr>
        <td>Dodano:</td>
        <td><strong>${offer.added}</strong></td>
      </tr>
    </tbody>
  </table>
  <a href="${offer.url}"> Przejdź do oferty </a>
  `;
  return html;
}

function renderOffer(offer) {
  //marker = L.marker(, { icon: getOfferIcon(offer) });
  marker = L.circleMarker([offer.lat, offer.lon], {
    color: "#f38181",
    radius: 7,
    fillOpacity: 1,
  }).bindPopup(getOfferHtml(offer));
  markers.addLayer(marker);
}

function getDateStr(date) {
  var dd = String(date.getDate()).padStart(2, "0");
  var mm = String(date.getMonth() + 1).padStart(2, "0");
  var yyyy = date.getFullYear();
  return yyyy + "-" + mm + "-" + dd;
}

function getFromDate() {
  var date = new Date();
  let days_ago = document.getElementById("from_date").value;
  date.setDate(date.getDate() - days_ago);
  return getDateStr(date);
}

function getTodayDate() {
  var today = new Date();
  return getDateStr(today);
}

function filterNullQueryParams(query) {
  query_without_blanks = {};
  for (const [key, value] of Object.entries(query)) {
    if (value) {
      query_without_blanks[key] = value;
    }
  }
  return query_without_blanks;
}

function updateOfferCount(count) {
  document.getElementById("offer_number").innerHTML = count;
}

async function refreshOffers() {
  markers.clearLayers();
  query = {
    from_date: getFromDate(),
    to_date: getTodayDate(),
    offer_type: document.getElementById("offer_type").value,
    max_price: document.getElementById("max_price").value,
    min_price: document.getElementById("min_price").value,
    max_size: document.getElementById("max_size").value,
    min_size: document.getElementById("min_size").value,
    min_price_estimate_diff: document.getElementById("min_price_estimate_diff")
      .value,
  };
  query = filterNullQueryParams(query);
  var url = new URL(API_URL + "/offers/");
  url.search = new URLSearchParams(query);
  const data = await getData(url.toString());
  updateOfferCount(data.length);
  data.forEach(renderOffer);
  markers.addTo(map);
}

async function Valuate() {
  // get location text
  var location = document.getElementById("valuate_location").value;
  var url =
    "https://api.geoapify.com/v1/geocode/search?text=" +
    location +
    "&lang=fr&limit=1&&filter=countrycode:pl&apiKey=3892daa71d7a4906adfd9c39696f8b35";
  var data = await getData(url);
  let coords = data.features[0].geometry.coordinates;
  query = {
    floor_n: document.getElementById("valuate_floor_n").value,
    floor: document.getElementById("valuate_floor").value,
    size: document.getElementById("valuate_size").value,
    building_year: document.getElementById("valuate_year").value,
    lat: coords[0],
    lon: coords[1]
  };

  query = filterNullQueryParams(query);
  var url = new URL(API_URL + "/predict/");
  url.search = new URLSearchParams(query);
  var prediction = await getData(url.toString());
  var price = roundLargeToThousands(parseFloat(prediction["prediction"]));
  document.getElementById("value").innerHTML = "To mieszkanie jest warte " + price + "zł";
}


refreshOffers();
