(function () {
  const slides = Array.from(document.querySelectorAll(".slide"));
  const deck = document.querySelector("#deck");
  const progress = document.querySelector(".progress-bar");
  const slideCount = document.querySelector(".slide-count");
  const prevButton = document.querySelector('[data-action="prev"]');
  const nextButton = document.querySelector('[data-action="next"]');

  function clampSlide(index) {
    return Math.max(0, Math.min(index, slides.length - 1));
  }

  function indexFromHash() {
    const id = window.location.hash.replace("#", "");
    const found = slides.findIndex((slide) => slide.id === id);
    return found === -1 ? 0 : found;
  }

  function setSlide(index, options = {}) {
    const nextIndex = clampSlide(index);

    slides.forEach((slide, slideIndex) => {
      slide.classList.toggle("active", slideIndex === nextIndex);
      slide.setAttribute("aria-hidden", String(slideIndex !== nextIndex));
    });

    const activeSlide = slides[nextIndex];
    const percent = ((nextIndex + 1) / slides.length) * 100;

    progress.style.width = `${percent}%`;
    slideCount.textContent = `${nextIndex + 1} / ${slides.length}`;
    document.title = `${activeSlide.dataset.title} - Solving the Dual Write Problem with Debezium`;

    prevButton.disabled = nextIndex === 0;
    nextButton.disabled = nextIndex === slides.length - 1;

    if (!options.skipHash) {
      history.pushState(null, "", `#${activeSlide.id}`);
    }

    deck.focus({ preventScroll: true });
  }

  function currentIndex() {
    return slides.findIndex((slide) => slide.classList.contains("active"));
  }

  function go(delta) {
    setSlide(currentIndex() + delta);
  }

  prevButton.addEventListener("click", () => go(-1));
  nextButton.addEventListener("click", () => go(1));

  document.addEventListener("keydown", (event) => {
    if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) {
      return;
    }

    const tag = event.target.tagName;
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
      return;
    }

    switch (event.key) {
      case "ArrowRight":
      case "PageDown":
      case " ":
        event.preventDefault();
        go(1);
        break;
      case "ArrowLeft":
      case "PageUp":
        event.preventDefault();
        go(-1);
        break;
      case "Home":
        event.preventDefault();
        setSlide(0);
        break;
      case "End":
        event.preventDefault();
        setSlide(slides.length - 1);
        break;
      default:
        break;
    }
  });

  window.addEventListener("hashchange", () => {
    setSlide(indexFromHash(), { skipHash: true });
  });

  setSlide(indexFromHash(), { skipHash: Boolean(window.location.hash) });
})();
