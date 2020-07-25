import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import FilterableFlatTable from './components/FilterableFlatTable';
import * as serviceWorker from './serviceWorker';


// "hack" with XML request is used to use json data from a public folder
var xhttp = new XMLHttpRequest();
var data = {};
xhttp.onreadystatechange = function() {
    if (this.readyState === 4 && this.status === 200) {
      // Typical action to be performed when the document is ready:
      data = JSON.parse(xhttp.responseText);
      ReactDOM.render(
        <React.StrictMode>
          <FilterableFlatTable flats={data} />
        </React.StrictMode>,
        document.getElementById('root'),
      );
    }
};

xhttp.open("GET", `${process.env.PUBLIC_URL}/data.json`, true);
xhttp.send();

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
