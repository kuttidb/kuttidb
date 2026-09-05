// Replay captured output, never simulate a live database in the browser.
(function () {
  'use strict';
  var output = document.getElementById('demo-output');
  var replay = document.getElementById('demo-replay');
  var result = document.getElementById('demo-result');
  var status = document.getElementById('demo-status');
  if (!output || !replay || !result || !status) return;

  fetch('demo-recording.json').then(function (response) {
    if (!response.ok) throw new Error('Recording unavailable');
    return response.json();
  }).then(function (recording) {
    var events = recording.events;
    if (recording.schema !== 1 || !Array.isArray(events) || !events.length ||
        events.length > 100 || !events.every(function (event, i) {
          return typeof event.text === 'string' && event.text.length < 1000 &&
            Number.isFinite(event.at) && event.at >= 0 && event.at < 300 &&
            (i === 0 || event.at >= events[i - 1].at);
        })) throw new Error('Invalid recording');
    var timer = null;
    var generation = 0;
    var transcript = events.map(function (event) { return event.text; }).join('\n');
    function finish() {
      generation += 1;
      clearTimeout(timer);
      output.textContent = transcript;
      output.scrollTop = output.scrollHeight;
      replay.textContent = 'Replay recording';
      status.textContent = 'Recording complete. Run the command to try it locally.';
    }
    replay.hidden = false;
    result.hidden = false;
    result.addEventListener('click', finish);
    replay.addEventListener('click', function () {
      // A reduced-motion preference keeps the transcript immediately readable.
      if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
        finish();
        return;
      }
      generation += 1;
      var current = generation;
      clearTimeout(timer);
      output.textContent = '';
      output.scrollTop = 0;
      replay.textContent = 'Restart recording';
      status.textContent = 'Playing a recorded local run...';
      var started = performance.now();
      function show(index) {
        if (current !== generation) return;
        output.textContent += (index ? '\n' : '') + events[index].text;
        output.scrollTop = output.scrollHeight;
        if (index + 1 === events.length) {
          finish();
        } else {
          var due = (events[index + 1].at - events[0].at) * 1000;
          timer = setTimeout(function () { show(index + 1); },
                             Math.max(0, due - (performance.now() - started)));
        }
      }
      show(0);
    });
    // Keep the full static transcript on initial load; never autoplay.
  }).catch(function () {
    status.textContent = 'Recorded output shown. Interactive replay is unavailable.';
  });
})();
