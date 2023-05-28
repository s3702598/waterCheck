mapboxgl.accessToken = 'pk.eyJ1IjoibG92aTEwIiwiYSI6ImNsaDJsZzdreDFlYjUzZnA3eXNjdTV4Z2MifQ.KWSJdNDfkEeRzVavXG7n6w';

// Define a function to get the marker color based on the beach status
function getMarkerColor(status) {
    if (status === 'SAFE') {
        return 'green';
    } else if (status === 'UNSAFE'){
        return 'red';
    } else {
        return 'gray'; // If the status is not found, show the marker in gray color
    }
}

function getCurrentDate() {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const day = now.getDate();
  return year + '-' + month + '-' + day;
}

// Add markers to the map and associate the status with each marker
var markers = [
    {name: 'Gibraltar Point Beach', coordinates: [-79.3803, 43.6149]},
    {name: 'Cherry Beach', coordinates: [-79.3468, 43.6362]},
    {name: 'Bluffer\'s Beach Park', coordinates: [-79.2233, 43.7108]},
    {name: 'Hanlan\'s Point Beach', coordinates: [-79.3858, 43.6184]},
    {name: 'Marie Curtis Park East Beach', coordinates: [-79.5394, 43.5888]},
    {name: 'Centre Island Beach', coordinates: [-79.3725, 43.6185]},
    {name: 'Ward\'s Island Beach', coordinates: [-79.3577, 43.6357]},
    {name: 'Woodbine Beaches', coordinates: [-79.3090, 43.6639]},
    {name: 'Kew Balmy Beach', coordinates: [-79.3109, 43.6562]},
    {name: 'Sunnyside Beach', coordinates: [-79.4594, 43.6351]}
];

 // Create the map
 var map = new mapboxgl.Map({
  container: 'map',
  style: 'mapbox://styles/mapbox/streets-v11',
  center: [-79.3832, 43.6532], // Toronto coordinates
  zoom: 10
});
console.log(map);

function renderMarkers() {
  fetch('/get_beach_status')
    .then(response => response.json())
    .then(data => {
      console.log(data);
      const beaches = Object.keys(data);
      beaches.forEach(beach => {
        const currentDate = new Date().toISOString().split('T')[0]; // get current date in 'YYYY-MM-DD' format
        const status = data[beach][currentDate];
        let color;
        let statusStr;
        if (status == 'UNSAFE') {
          color = 'red';
          statusStr = 'Unsafe'
        } else if (status == 'SAFE') {
          color = 'green';
          statusStr = 'Safe'
        } else {
          color = 'grey';
          statusStr = 'unknown'
        }
        console.log(`Color for ${beach} is ${color}`);
        console.log(`Status for ${beach} is ${statusStr}`);
        // Find the corresponding marker
        const marker = markers.find(m => m.name === beach);

        if (marker && marker.coordinates) {
          // Create the marker and add it to the map
          new mapboxgl.Marker({
            color : color
          })
          .setLngLat(marker.coordinates)
          .setPopup(new mapboxgl.Popup({ offset: 25 })
            .setHTML('<h3>' + marker.name + '</h3><h5>Status: ' + statusStr + '<h5>'))
          .addTo(map);
        } else {
          console.warn(`No coordinates found for beach ${beach}.`);
        } 
        });
    });
  }

  renderMarkers();



