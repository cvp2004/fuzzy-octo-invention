# REST API Reference

The kiwi-db Explorer exposes a full REST API for interacting with the LSM engine over HTTP. All endpoints are under the `/api/v1/` prefix.

<div id="swagger-ui"></div>

<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
document.addEventListener("DOMContentLoaded", function () {
  // Try live server first, fall back to static spec
  const liveUrl = window.location.protocol + "//" + window.location.hostname + ":8081/openapi.json";
  const staticUrl = "../assets/openapi.json";

  function initSwagger(url) {
    SwaggerUIBundle({
      url: url,
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout",
      deepLinking: true,
      defaultModelsExpandDepth: 1,
      docExpansion: "list",
      filter: true,
      tryItOutEnabled: true,
    });
  }

  // Try live server, fall back to static
  fetch(liveUrl, { mode: "cors" })
    .then(function (r) {
      if (r.ok) return initSwagger(liveUrl);
      throw new Error("not available");
    })
    .catch(function () {
      initSwagger(staticUrl);
    });
});
</script>

<style>
/* Blend Swagger UI into Material theme */
#swagger-ui { margin-top: 1rem; }
.swagger-ui .topbar { display: none; }
.swagger-ui .info { margin: 0 0 1rem 0; }
.swagger-ui .scheme-container { display: none; }

/* Dark mode support */
[data-md-color-scheme="slate"] .swagger-ui {
  filter: invert(88%) hue-rotate(180deg);
}
[data-md-color-scheme="slate"] .swagger-ui img {
  filter: invert(100%) hue-rotate(180deg);
}
</style>
