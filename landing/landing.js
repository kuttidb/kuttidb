(() => {
  'use strict';

  const tabs = [...document.querySelectorAll('[data-tab]')];
  const language = document.getElementById('example-language');
  const examples = window.kuttiExamples;
  const notes = {
    cache: 'A value when you need it. An expiry when you don’t.',
    queue: 'Acknowledge after processing. Unfinished work can be redelivered.',
    stream: 'Reading doesn’t remove an event. Come back to the same offset.'
  };
  // Highlight text nodes only. Snippets never become executable markup.
  function highlight(target, code) {
    target.replaceChildren();
    const tokens = /("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\/\/[^\n]*|#[^\n]*|\b(?:from|import|with|as|if|for|in|const|let|var|async|await|function|try|finally|return|new|public|class|static|void|throws|package|func|defer|range|use|fn|mut|Some|None|True|true|nil|null|int)\b|\b\d+(?:_\d+)*\b)/g;
    let position = 0;
    for (const match of code.matchAll(tokens)) {
      target.append(document.createTextNode(code.slice(position, match.index)));
      const token = document.createElement('span');
      token.className = match[0].startsWith('//') || match[0].startsWith('#') ? 'syntax-comment'
        : /^["']/.test(match[0]) ? 'syntax-string'
        : /^\d/.test(match[0]) ? 'syntax-number' : 'syntax-keyword';
      token.textContent = match[0];
      target.append(token);
      position = match.index + match[0].length;
    }
    target.append(document.createTextNode(code.slice(position)));
  }
  function updateLanguage() {
    const example = examples[language.value];
    const current = tabs.find(tab => tab.getAttribute('aria-selected') === 'true');
    tabs.forEach(tab => {
      const available = Boolean(example[tab.dataset.tab]);
      tab.disabled = !available;
      tab.setAttribute('aria-disabled', String(!available));
      const panel = document.getElementById(tab.getAttribute('aria-controls'));
      highlight(panel.querySelector('code'), example[tab.dataset.tab] || '');
      panel.querySelector('.code-note').textContent = notes[tab.dataset.tab];
      panel.querySelector('pre').scrollTop = 0;
      panel.querySelector('pre').scrollLeft = 0;
    });
    selectTab(current.disabled ? tabs[0] : current);
    document.getElementById('sdk-install').textContent = example.install;
    document.getElementById('sdk-install-label').textContent = language.value === 'java'
      ? 'MAVEN / POM.XML' : language.value === 'c' ? 'BUILD / FROM REPOSITORY' : 'INSTALL CLIENT';
    document.getElementById('sdk-note').textContent = example.note;
    document.querySelector('[role="tablist"]').setAttribute('aria-label', example.name + ' examples');
    document.getElementById('client-source').href = 'https://github.com/kuttidb/kuttidb/' +
      (language.value === 'c' ? 'blob' : 'tree') + '/main/' + example.source;
    document.getElementById('client-setup').href = 'https://github.com/kuttidb/kuttidb/blob/main/' +
      (language.value === 'c' ? 'docs/design/PROTOCOL.md#embedded-shared-memory-mode' : 'docs/guides/GETTING_STARTED.md');
  }
  function selectTab(tab, focus = false) {
    tabs.forEach(item => {
      const selected = item === tab;
      item.setAttribute('aria-selected', String(selected));
      item.tabIndex = selected ? 0 : -1;
      document.getElementById(item.getAttribute('aria-controls')).hidden = !selected;
    });
    if (focus) tab.focus();
  }
  tabs.forEach(tab => {
    tab.addEventListener('click', () => selectTab(tab));
    tab.addEventListener('keydown', event => {
      let next;
      const available = tabs.filter(item => !item.disabled);
      const index = available.indexOf(tab);
      if (event.key === 'ArrowRight') next = (index + 1) % available.length;
      if (event.key === 'ArrowLeft') next = (index + available.length - 1) % available.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = available.length - 1;
      if (next !== undefined) {
        event.preventDefault();
        selectTab(available[next], true);
      }
    });
  });
  if (examples) {
    language.disabled = false;
    language.addEventListener('change', updateLanguage);
    updateLanguage();
  }

  const copyStatus = document.getElementById('copy-status');
  document.querySelectorAll('[data-copy], [data-copy-example]').forEach(button => {
    // Clipboard access requires HTTPS or localhost. The commands stay selectable everywhere.
    if (!navigator.clipboard?.writeText) return;
    button.hidden = false;
    button.addEventListener('click', async () => {
      const target = button.hasAttribute('data-copy-example')
        ? document.querySelector('[role="tabpanel"]:not([hidden]) code')
        : document.getElementById(button.dataset.copy);
      const original = button.textContent;
      button.disabled = true;
      try {
        await navigator.clipboard.writeText(target.textContent.trim());
        button.textContent = 'Copied ✓';
        copyStatus.textContent = 'Copied to clipboard.';
      } catch {
        button.textContent = 'Select text to copy';
        copyStatus.textContent = 'Clipboard access is unavailable. Select and copy the text directly.';
        const range = document.createRange();
        range.selectNodeContents(target);
        const selection = window.getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
      }
      setTimeout(() => {
        button.textContent = original;
        button.disabled = false;
      }, 2200);
    });
  });

  const output = document.getElementById('demo-output');
  const replay = document.getElementById('demo-replay');
  const result = document.getElementById('demo-result');
  const status = document.getElementById('demo-status');
  // Replay the canonical recording; never fake a live server.
  fetch(new URL('demo-recording.json', document.currentScript.src))
    .then(response => {
      if (!response.ok) throw new Error('Recording unavailable');
      return response.json();
    })
    .then(recording => {
      const events = recording.events;
      if (recording.schema !== 1 || !Array.isArray(events) || !events.length ||
          events.length > 100 || !events.every((event, index) =>
            typeof event.text === 'string' && event.text.length < 1000 &&
            Number.isFinite(event.at) && event.at >= 0 && event.at < 300 &&
            (index === 0 || event.at >= events[index - 1].at))) {
        throw new Error('Invalid recording');
      }
      let timer;
      let generation = 0;
      const transcript = events.map(event => event.text).join('\n');
      output.textContent = transcript;
      replay.hidden = false;
      result.hidden = false;
      const finish = () => {
        generation += 1;
        clearTimeout(timer);
        output.textContent = transcript;
        output.scrollTop = output.scrollHeight;
        replay.textContent = 'Replay recording ↗';
        status.textContent = 'Recording complete. Try the command below to run it locally.';
      };
      result.addEventListener('click', finish);
      replay.addEventListener('click', () => {
        if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
          finish();
          return;
        }
        const current = ++generation;
        clearTimeout(timer);
        output.textContent = '';
        output.scrollTop = 0;
        replay.textContent = 'Restart recording ↗';
        status.textContent = 'Playing a recorded local run…';
        const started = performance.now();
        const show = index => {
          if (current !== generation) return;
          output.textContent += (index ? '\n' : '') + events[index].text;
          output.scrollTop = output.scrollHeight;
          if (index + 1 === events.length) finish();
          else {
            const due = (events[index + 1].at - events[0].at) * 1000;
            timer = setTimeout(() => show(index + 1), Math.max(0, due - (performance.now() - started)));
          }
        };
        show(0);
      });
    })
    .catch(() => {
      status.textContent = 'Recorded excerpt shown. Interactive replay is unavailable.';
    });
})();
