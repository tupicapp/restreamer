import Hls from "hls.js";
import { useEffect, useRef, useState } from "react";

const sideTools = [
  { id: "assets", label: "Assets", icon: "folder" },
  { id: "streams", label: "Streams", icon: "stream" },
  { id: "links", label: "Links", icon: "link" },
  { id: "destinations", label: "Destinations", icon: "grid" },
  { id: "chat", label: "Chat", icon: "chat" },
  { id: "settings", label: "Setting", icon: "gear", bottom: true }
];

const streamCards = [
  {
    id: "screen",
    title: "Stream screen",
    url: "rtmp://studio.local/screen",
    key: "scrn-4b3f-91d8",
    action: "screen",
    kind: "screen"
  },
  {
    id: "camera",
    title: "Stream camera",
    url: "rtmp://studio.local/camera",
    key: "camr-8c1a-34e1",
    action: "camera",
    kind: "camera"
  },
  {
    id: "guest",
    title: "Guest feed",
    url: "rtmp://studio.local/guest",
    key: "gues-21ac-91ff",
    action: "camera",
    kind: "camera"
  },
  {
    id: "slides",
    title: "Slides input",
    url: "rtmp://studio.local/slides",
    key: "slds-74aa-2dc1",
    action: "screen",
    kind: "screen"
  },
  {
    id: "backup",
    title: "Backup scene",
    url: "rtmp://studio.local/backup",
    key: "bkup-0af3-11d2",
    action: "screen",
    kind: "screen"
  }
];

const hourMarks = Array.from({ length: 12 }, (_, index) => index * 2);
const initialStudioState = {
  channel_id: "",
  streams: [],
  selected_stream_id: "camera",
  stream_mode: "idle",
  timeline_items: []
};
const initialWorkspaceState = {
  channel_id: "",
  studio: initialStudioState,
  manifest: null,
  runner: {
    is_started: false,
    manifest_version: "",
    plan_kind: "",
    active_scene_id: "",
    current_input_id: "",
    current_input_url: "",
    preview_url: "",
    outputs: []
  }
};
const emptyEditorState = {
  id: "",
  source_type: "link",
  source_id: "",
  url: "",
  starts_at_local: "",
  finishes_at_local: "",
  infinite: false,
  details_text: "{}"
};

function App() {
  const channelID = getChannelIDFromPath(window.location.pathname);
  const [activeTool, setActiveTool] = useState("streams");
  const [drawerOpen, setDrawerOpen] = useState(true);
  const [cameraModalOpen, setCameraModalOpen] = useState(false);
  const [now, setNow] = useState(() => new Date());
  const [timelineSelectionPercent, setTimelineSelectionPercent] = useState(() =>
    getLocalDayProgress(new Date())
  );
  const [timelineZoomHours, setTimelineZoomHours] = useState(24);
  const [timelineViewStartUnix, setTimelineViewStartUnix] = useState(null);
  const [isDraggingSelection, setIsDraggingSelection] = useState(false);
  const [workspace, setWorkspace] = useState(initialWorkspaceState);
  const [draftManifest, setDraftManifest] = useState(null);
  const [selectedElementID, setSelectedElementID] = useState("");
  const [editorState, setEditorState] = useState(emptyEditorState);
  const [resizeState, setResizeState] = useState(null);
  const [apiError, setAPIError] = useState("");
  const [saveErrorPopup, setSaveErrorPopup] = useState("");
  const trackLaneRef = useRef(null);
  const previewVideoRef = useRef(null);

  const studio = workspace.studio ?? initialStudioState;
  const runner = workspace.runner ?? initialWorkspaceState.runner;
  const streamCardsFromAPI = studio.streams.length > 0 ? studio.streams : streamCards;
  const selectedStreamId = studio.selected_stream_id || "camera";
  const streamMode = studio.stream_mode || "idle";
  const resolvedChannelID = workspace.channel_id || channelID;
  const channelDashboardPath = resolvedChannelID ? `/${resolvedChannelID}` : "/";
  const timelineBaseDate = getTimelineBaseDate(draftManifest, now);
  const dayStartUnix = Math.round(timelineBaseDate.getTime() / 1000);
  const visibleRangeSeconds = timelineZoomHours * 3600;
  const effectiveViewStartUnix = normalizeViewStartUnix(
    timelineViewStartUnix ?? dayStartUnix,
    dayStartUnix,
    visibleRangeSeconds
  );
  const effectiveViewEndUnix = effectiveViewStartUnix + visibleRangeSeconds;
  const currentTimeLabel = formatLocalTime(now);
  const currentDateLabel = formatLocalDate(timelineBaseDate);
  const currentZoneLabel = formatLocalZone(now);
  const currentCursorPercent = getLocalDayProgress(now);
  const selectedTimelineTimeLabel = formatTimelinePoint(timelineSelectionPercent, timelineBaseDate);
  const selectedStreamCard =
    streamCardsFromAPI.find((card) => card.id === selectedStreamId) ?? streamCardsFromAPI[0];
  const timelineElements = getManifestElements(draftManifest);
  const selectedElement = findManifestElement(draftManifest, selectedElementID);
  const primaryOutput = pickPrimaryRunnerOutput(runner);
  const outputPath = primaryOutput?.local_path || "";
  const outputFile = primaryOutput?.url || runner.preview_url || "";
  const previewURL = runner.preview_url || primaryOutput?.url || "";
  const outputSummary = outputPath || outputFile || runner.current_input_url || "";
  const canRenderPreview = isPlayablePreviewURL(previewURL);
  const hasUnsavedChanges = !manifestsEqual(draftManifest, workspace.manifest);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setNow(new Date());
    }, 1000);

    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!channelID) {
      setAPIError("Channel id is missing from the URL path.");
      return;
    }
    void loadWorkspace(channelID);
  }, [channelID]);

  useEffect(() => {
    setTimelineViewStartUnix(dayStartUnix);
  }, [dayStartUnix]);

  useEffect(() => {
    if (!selectedElement) {
      setEditorState(emptyEditorState);
      return;
    }
    setEditorState(buildEditorState(selectedElement));
  }, [selectedElementID, draftManifest]);

  useEffect(() => {
    const video = previewVideoRef.current;
    if (!video || !canRenderPreview || !previewURL) {
      return undefined;
    }

    video.defaultMuted = false;
    video.muted = false;
    video.volume = 1;

    if (video.canPlayType("application/vnd.apple.mpegurl")) {
      video.src = previewURL;
      void video.play().catch(() => {});
      return () => {
        video.removeAttribute("src");
        video.load();
      };
    }

    if (Hls.isSupported()) {
      const hls = new Hls({
        enableWorker: true,
        lowLatencyMode: true
      });
      hls.loadSource(previewURL);
      hls.attachMedia(video);
      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        video.defaultMuted = false;
        video.muted = false;
        video.volume = 1;
        void video.play().catch(() => {});
      });
      return () => {
        hls.destroy();
      };
    }

    return undefined;
  }, [canRenderPreview, previewURL]);

  useEffect(() => {
    if (!isDraggingSelection && !resizeState) {
      return undefined;
    }

    const handlePointerMove = (event) => {
      if (resizeState) {
        updateResize(event.clientX, resizeState);
        return;
      }
      updateTimelineSelection(event.clientX);
    };

    const handlePointerUp = () => {
      setIsDraggingSelection(false);
      setResizeState(null);
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", handlePointerUp);

    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", handlePointerUp);
    };
  }, [isDraggingSelection, resizeState, draftManifest, timelineBaseDate]);

  const loadWorkspace = async (nextChannelID) => {
    try {
      setAPIError("");
      const nextState = await studioRequest(channelWorkspacePath(nextChannelID));
      const normalized = normalizeWorkspaceState(nextState, nextChannelID);
      setWorkspace(normalized);
      setDraftManifest(normalized.manifest);
      setSelectedElementID(getManifestElements(normalized.manifest)[0]?.id ?? "");
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const selectStream = async (streamID) => {
    try {
      setAPIError("");
      const nextState = await studioRequest(channelStudioPath(channelID, "select"), {
        method: "POST",
        body: JSON.stringify({ stream_id: streamID })
      });
      setWorkspace((current) => ({
        ...current,
        channel_id: resolvedChannelID,
        studio: normalizeStudioState(nextState, resolvedChannelID)
      }));
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const openStream = async (streamCard) => {
    try {
      setAPIError("");
      const nextState = await studioRequest(channelStudioPath(channelID, "open"), {
        method: "POST",
        body: JSON.stringify({ stream_id: streamCard.id })
      });
      setWorkspace((current) => ({
        ...current,
        channel_id: resolvedChannelID,
        studio: normalizeStudioState(nextState, resolvedChannelID)
      }));
      setCameraModalOpen(streamCard.action === "camera");
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const stopAll = async () => {
    try {
      setAPIError("");
      await studioRequest(channelStudioPath(channelID, "stop"), {
        method: "POST"
      });
      await loadWorkspace(channelID);
      setCameraModalOpen(false);
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const updateTimelineSelection = (clientX) => {
    const lane = trackLaneRef.current;
    if (!lane) {
      return;
    }
    const rect = lane.getBoundingClientRect();
    const nextVisiblePercent = clamp(((clientX - rect.left) / rect.width) * 100, 0, 100);
    const nextUnix = visiblePercentToUnix(nextVisiblePercent, effectiveViewStartUnix, visibleRangeSeconds);
    setTimelineSelectionPercent(unixToTimelinePercent(nextUnix, timelineBaseDate));
  };

  const updateResize = (clientX, nextResizeState) => {
    const lane = trackLaneRef.current;
    if (!lane) {
      return;
    }
    const rect = lane.getBoundingClientRect();
    const nextPercent = clamp(((clientX - rect.left) / rect.width) * 100, 0, 100);
    const nextUnix = visiblePercentToUnix(nextPercent, effectiveViewStartUnix, visibleRangeSeconds);
    setDraftManifest((current) =>
      resizeManifestElement(current, nextResizeState.elementID, nextResizeState.edge, nextUnix)
    );
  };

  const zoomTimeline = (direction) => {
    const nextZoomHours = clampZoomHours(
      direction === "in" ? timelineZoomHours / 2 : timelineZoomHours * 2
    );
    const selectionUnix = timelinePercentToUnix(timelineSelectionPercent, timelineBaseDate);
    const nextRangeSeconds = nextZoomHours * 3600;
    const nextStart = normalizeViewStartUnix(
      Math.round(selectionUnix - nextRangeSeconds / 2),
      dayStartUnix,
      nextRangeSeconds
    );
    setTimelineZoomHours(nextZoomHours);
    setTimelineViewStartUnix(nextStart);
  };

  const panTimeline = (direction) => {
    const step = visibleRangeSeconds / 2;
    const delta = direction === "left" ? -step : step;
    setTimelineViewStartUnix(
      normalizeViewStartUnix(effectiveViewStartUnix + delta, dayStartUnix, visibleRangeSeconds)
    );
  };

  const addTimelineItem = (streamCard = selectedStreamCard) => {
    try {
      setAPIError("");
      const insertAt = timelinePercentToUnix(timelineSelectionPercent, timelineBaseDate);
      const { manifest: nextManifest, elementID } = insertElementIntoManifest(
        draftManifest,
        resolvedChannelID,
        streamCard,
        insertAt
      );
      setDraftManifest(nextManifest);
      setSelectedElementID(elementID);
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const saveManifest = async (nextManifest, nextSelectedElementID = selectedElementID) => {
    const manifestToSave = nextManifest ?? draftManifest;
    if (!manifestToSave) {
      return;
    }

    try {
      setAPIError("");
      setSaveErrorPopup("");
      const nextState = await studioRequest(channelManifestPath(resolvedChannelID), {
        method: "PUT",
        body: JSON.stringify(manifestToSave)
      });
      const normalized = normalizeWorkspaceState(nextState, resolvedChannelID);
      setWorkspace(normalized);
      setDraftManifest(normalized.manifest);
      setSelectedElementID(nextSelectedElementID);
    } catch (error) {
      setAPIError(error.message);
      setSaveErrorPopup(error.message);
    }
  };

  const updateEditorField = (field, value) => {
    setEditorState((current) => ({
      ...current,
      [field]: value
    }));
  };

  const confirmEditorChanges = () => {
    if (!selectedElementID || !draftManifest) {
      return;
    }

    try {
      setAPIError("");
      const parsedDetails = parseDetailsJSON(editorState.details_text);
      const nextManifest = updateManifestElement(draftManifest, selectedElementID, (element) => ({
        ...element,
        id: editorState.id.trim(),
        source_type: editorState.source_type.trim(),
        source_id: editorState.source_id.trim(),
        url: editorState.url.trim(),
        starts_at: parseLocalDateTimeInput(editorState.starts_at_local),
        finishes_at: editorState.infinite ? -1 : parseLocalDateTimeInput(editorState.finishes_at_local),
        details: parsedDetails
      }));
      setDraftManifest(nextManifest);
    } catch (error) {
      setAPIError(error.message);
    }
  };

  const handleTimelineElementClick = (element) => {
    void selectStream(element.source_id || selectedStreamId);
    setSelectedElementID(element.id);
    setTimelineSelectionPercent(unixToTimelinePercent(element.starts_at, timelineBaseDate));
  };

  if (!channelID) {
    return (
      <div className="studio-shell">
        <div className="ambient ambient-a" />
        <div className="ambient ambient-b" />
        <div className="channel-empty panel">
          <strong>Channel id missing</strong>
          <p>Open the studio as `http://localhost:4173/&lt;channel_id&gt;`.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="studio-shell">
      <div className="ambient ambient-a" />
      <div className="ambient ambient-b" />
      <header className="topbar panel">
        <div className="brand">
          <div className="brand-mark">
            <span />
            <span />
            <span />
          </div>
          <div className="brand-text">
            <span className="brand-primary">Tupic</span>
            <span className="brand-live">Live</span>
          </div>
        </div>
        <nav className="topnav">
          <a href="/">Explore</a>
          <a href={channelDashboardPath} className="active">
            Studio
          </a>
          <a href={channelDashboardPath}>Channel</a>
        </nav>
        <div className="topbar-divider" />
        <div className="top-actions">
          <div className="channel-pill">Channel {resolvedChannelID}</div>
          <button className="icon-button" type="button" aria-label="Notifications">
            <BellIcon />
          </button>
          <button className="avatar" type="button" aria-label="Profile">
            NL
          </button>
        </div>
      </header>

      <div className="main-grid">
        <aside className="scene-rail panel">
          <div className="rail-title">Scenes</div>
          <div className="rail-subtitle">Channel {resolvedChannelID}</div>
          <button className="scene-card selected" type="button">
            <div className="scene-thumb">
              <ImageIcon />
            </div>
            <div className="scene-meta">
              <span>{draftManifest?.scenes?.[0]?.id ?? "One screen"}</span>
              <DotsIcon />
            </div>
          </button>
        </aside>

        <main className="workspace">
          <section className="preview-area">
            <div className="preview-frame panel">
              <div className="preview-topline">Playing Track 1</div>
              <div className="preview-canvas">
                <div className="hero-card">
                  <div className="hero-copy">
                    <h1>Channel {resolvedChannelID}</h1>
                    <p>
                      {runner.is_started
                        ? outputSummary || "Runner started without a served output URL yet."
                        : "Load or start a channel manifest to get a backend output state here."}
                    </p>
                  </div>
                  <div className="hero-actions">
                    <button
                      type="button"
                      className="action-pill"
                      onClick={() => void openStream(findStreamCard(streamCardsFromAPI, "camera"))}
                    >
                      <CameraIcon />
                      Camera Stream
                    </button>
                    <button
                      type="button"
                      className="action-pill"
                      onClick={() => void openStream(findStreamCard(streamCardsFromAPI, "screen"))}
                    >
                      <ScreenIcon />
                      Screen Stream
                    </button>
                  </div>
                </div>
                {canRenderPreview ? (
                  <video
                    ref={previewVideoRef}
                    className="preview-player"
                    controls
                    autoPlay
                    playsInline
                  />
                ) : null}
                <div className={`preview-overlay ${runner.is_started ? "active" : ""}`}>
                  <div className={`source-indicator ${runner.is_started ? "live" : ""}`}>
                    {runner.is_started
                      ? outputSummary || "Runner started"
                      : `No backend output available for channel ${resolvedChannelID}`}
                  </div>
                </div>
                <div className="preview-state panel">
                  <div className="preview-state-row">
                    <span>Status</span>
                    <strong>{runner.is_started ? "Started" : "Stopped"}</strong>
                  </div>
                  <div className="preview-state-row">
                    <span>Active scene</span>
                    <strong>{runner.active_scene_id || "none"}</strong>
                  </div>
                  <div className="preview-state-row">
                    <span>Output File</span>
                    <strong>{outputFile || "none"}</strong>
                  </div>
                  <div className="preview-state-row">
                    <span>Output Path</span>
                    <strong>{outputPath || "none"}</strong>
                  </div>
                </div>
              </div>
            </div>

            {drawerOpen ? (
              <aside className="workspace-sidebar">
                <section className="streams-drawer panel">
                  <div className="drawer-head">
                    <strong>Streams</strong>
                    <button
                      type="button"
                      className="icon-button muted"
                      onClick={() => setDrawerOpen(false)}
                      aria-label="Close streams panel"
                    >
                      <CloseIcon />
                    </button>
                  </div>
                  <div className="drawer-list">
                    {streamCardsFromAPI.map((card) => (
                      <article className="stream-card" key={card.id}>
                        <button
                          type="button"
                          className={`stream-card-head stream-select ${
                            selectedStreamId === card.id ? "selected" : ""
                          }`}
                          onClick={() => {
                            void selectStream(card.id);
                          }}
                        >
                          <span>{card.title}</span>
                          <DotsIcon />
                        </button>
                        <div className="stream-row">
                          <label>URL</label>
                          <div className="value">
                            <span>{card.url}</span>
                            <CopyIcon />
                          </div>
                        </div>
                        <div className="stream-row">
                          <label>Key</label>
                          <div className="value">
                            <span>{card.key}</span>
                            <CopyIcon />
                          </div>
                        </div>
                        <div className="stream-card-actions">
                          <button
                            type="button"
                            className="ghost-pill slim"
                            onClick={() => {
                              void selectStream(card.id);
                              addTimelineItem(card);
                            }}
                          >
                            <PlusIcon />
                            Add to timeline
                          </button>
                          <button
                            type="button"
                            className="ghost-pill slim"
                            onClick={() => {
                              void openStream(card);
                            }}
                          >
                            {card.action === "camera" ? <CameraIcon /> : <ScreenIcon />}
                            Open
                          </button>
                        </div>
                      </article>
                    ))}
                  </div>
                </section>

                <section className="element-editor panel">
                  <div className="drawer-head">
                    <strong>Element</strong>
                    <span className="editor-status">
                      {selectedElement ? selectedElement.id || "selected" : "No selection"}
                    </span>
                  </div>
                  {selectedElement ? (
                    <div className="editor-form">
                      <label className="editor-field">
                        <span>ID</span>
                        <input
                          value={editorState.id}
                          onChange={(event) => updateEditorField("id", event.target.value)}
                        />
                      </label>
                      <label className="editor-field">
                        <span>Source Type</span>
                        <input
                          value={editorState.source_type}
                          onChange={(event) => updateEditorField("source_type", event.target.value)}
                        />
                      </label>
                      <label className="editor-field">
                        <span>Source ID</span>
                        <input
                          value={editorState.source_id}
                          onChange={(event) => updateEditorField("source_id", event.target.value)}
                        />
                      </label>
                      <label className="editor-field">
                        <span>URL</span>
                        <input
                          value={editorState.url}
                          onChange={(event) => updateEditorField("url", event.target.value)}
                        />
                      </label>
                      <div className="editor-grid">
                        <label className="editor-field">
                          <span>Starts At</span>
                          <input
                            type="datetime-local"
                            step="1"
                            value={editorState.starts_at_local}
                            onChange={(event) => updateEditorField("starts_at_local", event.target.value)}
                          />
                        </label>
                        <label className="editor-field">
                          <span>Finishes At</span>
                          <input
                            type="datetime-local"
                            step="1"
                            value={editorState.finishes_at_local}
                            onChange={(event) => updateEditorField("finishes_at_local", event.target.value)}
                            disabled={editorState.infinite}
                          />
                        </label>
                      </div>
                      <label className="editor-check">
                        <input
                          type="checkbox"
                          checked={editorState.infinite}
                          onChange={(event) => updateEditorField("infinite", event.target.checked)}
                        />
                        Infinite end (`-1`)
                      </label>
                      <label className="editor-field">
                        <span>Details JSON</span>
                        <textarea
                          value={editorState.details_text}
                          onChange={(event) => updateEditorField("details_text", event.target.value)}
                        />
                      </label>
                      <button type="button" className="drawer-add editor-save" onClick={confirmEditorChanges}>
                        Apply changes
                      </button>
                    </div>
                  ) : (
                    <div className="editor-empty">
                      Click a timeline element to inspect and edit its full manifest fields.
                    </div>
                  )}
                </section>
              </aside>
            ) : null}
          </section>

          <section className="timeline panel">
            <div className="timeline-head">
              <div>
                <div className="timeline-title">Channel Output</div>
                <div className="timeline-subtitle">
                  Channel {resolvedChannelID} · {currentDateLabel} · {currentZoneLabel}
                </div>
              </div>
              <div className="timeline-status">
                <span>{currentTimeLabel}</span>
                <button
                  type="button"
                  className="ghost-pill workspace-save"
                  onClick={() => void saveManifest()}
                  disabled={!hasUnsavedChanges}
                >
                  Save
                </button>
                <button type="button" className="danger-pill" onClick={() => void stopAll()}>
                  Stop broadcast
                </button>
              </div>
            </div>
            <div className="timeline-scale">
              <div className="timeline-zoom">
                <button type="button" className="ghost-mini" onClick={() => zoomTimeline("out")}>
                  <MinusIcon />
                  Zoom out
                </button>
                <button type="button" className="ghost-mini" onClick={() => zoomTimeline("in")}>
                  <PlusIcon />
                  Zoom in
                </button>
              </div>
              <div className="timeline-days">
                <span>{currentDateLabel}</span>
                <button type="button" className="mini-arrow" onClick={() => panTimeline("left")}>
                  <ArrowLeftIcon />
                </button>
                <button type="button" className="mini-arrow" onClick={() => panTimeline("right")}>
                  <ArrowRightIcon />
                </button>
              </div>
            </div>
            <div className="timeline-hours">
              <span className="zoom-level">{formatZoomLabel(timelineZoomHours)}</span>
              {buildHourMarksForView(effectiveViewStartUnix, visibleRangeSeconds).map((hour) => (
                <span key={hour.key}>{hour.label}</span>
              ))}
            </div>
            <div className="timeline-track-row">
              <button type="button" className="track-badge">
                track 1 <DotsIcon />
              </button>
              <div
                ref={trackLaneRef}
                className="track-lane interactive"
                onPointerDown={(event) => {
                  updateTimelineSelection(event.clientX);
                  setIsDraggingSelection(true);
                }}
              >
                <div className="track-grid" />
                <div
                  className="track-cursor"
                  style={{ left: `${unixToVisiblePercent(nowUnix(now), effectiveViewStartUnix, visibleRangeSeconds)}%` }}
                />
                <div
                  className="track-current-time"
                  style={{ left: `${unixToVisiblePercent(nowUnix(now), effectiveViewStartUnix, visibleRangeSeconds)}%` }}
                >
                  {currentTimeLabel}
                </div>
                <div
                  className="track-selection-cursor"
                  style={{
                    left: `${unixToVisiblePercent(
                      timelinePercentToUnix(timelineSelectionPercent, timelineBaseDate),
                      effectiveViewStartUnix,
                      visibleRangeSeconds
                    )}%`
                  }}
                />
                <div
                  className="track-selection-time"
                  style={{
                    left: `${unixToVisiblePercent(
                      timelinePercentToUnix(timelineSelectionPercent, timelineBaseDate),
                      effectiveViewStartUnix,
                      visibleRangeSeconds
                    )}%`
                  }}
                >
                  {selectedTimelineTimeLabel}
                </div>
                <button
                  type="button"
                  className="track-add-button"
                  style={{
                    left: `${unixToVisiblePercent(
                      timelinePercentToUnix(timelineSelectionPercent, timelineBaseDate),
                      effectiveViewStartUnix,
                      visibleRangeSeconds
                    )}%`
                  }}
                  onPointerDown={(event) => event.stopPropagation()}
                  onClick={(event) => {
                    event.stopPropagation();
                    addTimelineItem();
                  }}
                  aria-label="Add selected stream to timeline"
                >
                  <PlusIcon />
                </button>
                {timelineElements.map((element) => {
                  const startPercent = unixToVisiblePercent(
                    element.starts_at,
                    effectiveViewStartUnix,
                    visibleRangeSeconds
                  );
                  const endPercent =
                    element.finishes_at === -1
                      ? 100
                      : unixToVisiblePercent(
                          element.finishes_at,
                          effectiveViewStartUnix,
                          visibleRangeSeconds
                        );
                  if (endPercent <= 0 || startPercent >= 100) {
                    return null;
                  }
                  const widthPercent = Math.max(endPercent - startPercent, 2);
                  return (
                    <div
                      key={element.id}
                      className={`track-segment ${element.kind} ${
                        selectedElementID === element.id ? "active" : ""
                      }`}
                      style={{
                        left: `${startPercent}%`,
                        width: `${widthPercent}%`
                      }}
                      onPointerDown={(event) => event.stopPropagation()}
                      onClick={(event) => {
                        event.stopPropagation();
                        handleTimelineElementClick(element);
                      }}
                    >
                      <button
                        type="button"
                        className="track-handle left"
                        aria-label={`Resize start for ${element.id}`}
                        onPointerDown={(event) => {
                          event.stopPropagation();
                          setSelectedElementID(element.id);
                          setResizeState({ elementID: element.id, edge: "left" });
                        }}
                      />
                      <span className="track-segment-label">{element.label}</span>
                      <button
                        type="button"
                        className="track-handle right"
                        aria-label={`Resize end for ${element.id}`}
                        onPointerDown={(event) => {
                          event.stopPropagation();
                          setSelectedElementID(element.id);
                          setResizeState({ elementID: element.id, edge: "right" });
                        }}
                      />
                    </div>
                  );
                })}
                {timelineElements.length === 0 ? (
                  <div className="timeline-empty">
                    No manifest elements yet. Select a stream and add it to the timeline.
                  </div>
                ) : null}
                {streamMode !== "idle" ? (
                  <div className={`track-live-badge ${streamMode}`}>
                    Live: {streamMode === "camera" ? "Camera" : "Screen"}
                  </div>
                ) : null}
              </div>
            </div>
            {apiError ? <div className="api-error">{apiError}</div> : null}
          </section>
        </main>

        <aside className="tool-rail panel">
          <div className="tool-rail-main">
            {sideTools
              .filter((tool) => !tool.bottom)
              .map((tool) => (
                <button
                  key={tool.id}
                  type="button"
                  className={`tool-button ${activeTool === tool.id ? "active" : ""}`}
                  onClick={() => {
                    setActiveTool(tool.id);
                    if (tool.id === "streams") {
                      setDrawerOpen(true);
                    }
                  }}
                >
                  <ToolIcon type={tool.icon} />
                  <span>{tool.label}</span>
                </button>
              ))}
          </div>
          <button
            type="button"
            className={`tool-button ${activeTool === "settings" ? "active" : ""}`}
            onClick={() => setActiveTool("settings")}
          >
            <ToolIcon type="gear" />
            <span>Setting</span>
          </button>
        </aside>
      </div>

      {cameraModalOpen ? (
        <div className="modal-backdrop" onClick={() => setCameraModalOpen(false)} role="presentation">
          <div className="camera-modal panel" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true">
            <div className="modal-head">
              <strong>Stream camera</strong>
              <button type="button" className="icon-button muted" onClick={() => setCameraModalOpen(false)}>
                <CloseIcon />
              </button>
            </div>
            <div className="camera-feed">
              <div className="feed-badge">{currentTimeLabel}</div>
              <div className="feed-viewers">
                <EyeIcon />
                123
              </div>
              <div className="camera-scene">
                <div className="light-strip left" />
                <div className="light-strip right" />
                <div className="screen-glow" />
                <div className="subject main" />
                <div className="subject secondary" />
                <div className="desk" />
              </div>
            </div>
            <div className="modal-controls">
              <button type="button" className="icon-button">
                <CameraIcon />
              </button>
              <button type="button" className="icon-button">
                <MicIcon />
              </button>
              <button type="button" className="icon-button">
                <UsersIcon />
              </button>
              <button type="button" className="danger-pill small" onClick={() => void stopAll()}>
                Stop stream
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {saveErrorPopup ? (
        <div className="modal-backdrop" onClick={() => setSaveErrorPopup("")} role="presentation">
          <div className="error-popup panel" onClick={(event) => event.stopPropagation()} role="alertdialog" aria-modal="true">
            <div className="modal-head">
              <strong>Manifest Save Failed</strong>
              <button type="button" className="icon-button muted" onClick={() => setSaveErrorPopup("")}>
                <CloseIcon />
              </button>
            </div>
            <p>{saveErrorPopup}</p>
            <div className="error-popup-actions">
              <button type="button" className="danger-pill small" onClick={() => setSaveErrorPopup("")}>
                Close
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

async function studioRequest(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {})
    },
    ...options
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || "request failed");
  }
  return data;
}

function getChannelIDFromPath(pathname) {
  return pathname
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)[0] ?? "";
}

function channelStudioPath(channelID, action) {
  return `/api/channels/${encodeURIComponent(channelID)}/studio/${action}`;
}

function channelWorkspacePath(channelID) {
  return `/api/channels/${encodeURIComponent(channelID)}/workspace`;
}

function channelManifestPath(channelID) {
  return `/api/channels/${encodeURIComponent(channelID)}/manifest`;
}

function normalizeWorkspaceState(value, fallbackChannelID) {
  const studio = normalizeStudioState(value?.studio, fallbackChannelID);
  return {
    channel_id: value?.channel_id ?? fallbackChannelID ?? "",
    studio,
    manifest: normalizeManifest(value?.manifest, fallbackChannelID),
    runner: normalizeRunnerState(value?.runner)
  };
}

function normalizeStudioState(value, fallbackChannelID) {
  return {
    channel_id: value?.channel_id ?? fallbackChannelID ?? "",
    streams: Array.isArray(value?.streams) ? value.streams : [],
    selected_stream_id: value?.selected_stream_id ?? "camera",
    stream_mode: value?.stream_mode ?? "idle",
    timeline_items: Array.isArray(value?.timeline_items) ? value.timeline_items : []
  };
}

function normalizeManifest(value, fallbackChannelID) {
  if (!value) {
    return null;
  }
  return {
    ...value,
    channel_id: value.channel_id ?? fallbackChannelID ?? "",
    scenes: Array.isArray(value.scenes) ? value.scenes : []
  };
}

function normalizeRunnerState(value) {
  return {
    is_started: value?.is_started ?? false,
    manifest_version: value?.manifest_version ?? "",
    plan_kind: value?.plan_kind ?? "",
    active_scene_id: value?.active_scene_id ?? "",
    current_input_id: value?.current_input_id ?? "",
    current_input_url: value?.current_input_url ?? "",
    preview_url: value?.preview_url ?? "",
    outputs: Array.isArray(value?.outputs) ? value.outputs : []
  };
}

function pickPrimaryRunnerOutput(runner) {
  const outputs = Array.isArray(runner?.outputs) ? runner.outputs : [];
  if (outputs.length === 0) {
    return null;
  }
  return outputs[0];
}

function isPlayablePreviewURL(value) {
  const lower = String(value || "").toLowerCase();
  return lower.startsWith("http://") || lower.startsWith("https://") || lower.startsWith("/hls/");
}

function buildEditorState(element) {
  return {
    id: element.id ?? "",
    source_type: element.source_type ?? "link",
    source_id: element.source_id ?? "",
    url: element.url ?? "",
    starts_at_local: formatLocalDateTimeInput(element.starts_at),
    finishes_at_local: element.finishes_at === -1 ? "" : formatLocalDateTimeInput(element.finishes_at),
    infinite: element.finishes_at === -1,
    details_text: JSON.stringify(element.details ?? {}, null, 2)
  };
}

function parseDetailsJSON(raw) {
  const trimmed = raw.trim();
  if (trimmed === "") {
    return {};
  }
  const parsed = JSON.parse(trimmed);
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("details must be a JSON object");
  }
  return parsed;
}

function getManifestElements(manifest) {
  const elements = manifest?.scenes?.[0]?.slots?.[0]?.elements ?? [];
  return elements.map((element, index) => ({
    ...element,
    kind: inferStreamKind(element),
    label: element.id || element.source_id || element.url || `element-${index + 1}`
  }));
}

function findManifestElement(manifest, elementID) {
  if (!elementID) {
    return null;
  }
  return getManifestElements(manifest).find((element) => element.id === elementID) ?? null;
}

function updateManifestElement(manifest, elementID, updater) {
  if (!manifest) {
    return manifest;
  }
  return {
    ...manifest,
    scenes: manifest.scenes.map((scene, sceneIndex) => {
      if (sceneIndex !== 0) {
        return scene;
      }
      return {
        ...scene,
        slots: scene.slots.map((slot, slotIndex) => {
          if (slotIndex !== 0) {
            return slot;
          }
          return {
            ...slot,
            elements: slot.elements.map((element) =>
              element.id === elementID ? updater({ ...element }) : element
            )
          };
        })
      };
    })
  };
}

function resizeManifestElement(manifest, elementID, edge, nextUnix) {
  if (!manifest) {
    return manifest;
  }

  const next = cloneManifest(manifest);
  const elements = next.scenes[0].slots[0].elements;
  const index = elements.findIndex((element) => element.id === elementID);
  if (index === -1) {
    return manifest;
  }

  const current = { ...elements[index] };
  const previous = index > 0 ? { ...elements[index - 1] } : null;
  const upcoming = index < elements.length - 1 ? { ...elements[index + 1] } : null;

  if (edge === "left") {
    const minStart = previous ? previous.starts_at : 1;
    const maxStart = current.finishes_at === -1 ? current.starts_at + 86400 : current.finishes_at - 1;
    const clamped = clampInt(nextUnix, minStart, maxStart);
    current.starts_at = clamped;
    if (previous) {
      previous.finishes_at = clamped;
      elements[index - 1] = previous;
    }
  } else {
    if (upcoming) {
      const minFinish = current.starts_at + 1;
      const maxFinish = upcoming.finishes_at === -1 ? upcoming.starts_at : upcoming.starts_at;
      const clamped = clampInt(nextUnix, minFinish, maxFinish);
      current.finishes_at = clamped;
      upcoming.starts_at = clamped;
      elements[index + 1] = upcoming;
    } else {
      const minFinish = current.starts_at + 1;
      current.finishes_at = Math.max(nextUnix, minFinish);
    }
  }

  elements[index] = current;
  return next;
}

function insertElementIntoManifest(manifest, channelID, streamCard, insertAtUnix) {
  const next = manifest ? cloneManifest(manifest) : buildDefaultManifest(channelID, streamCard, insertAtUnix);
  if (!manifest) {
    return {
      manifest: next,
      elementID: next.scenes[0].slots[0].elements[0].id
    };
  }

  const elements = next.scenes[0].slots[0].elements;
  const newID = `${streamCard.id}-${insertAtUnix}`;
  const newElement = {
    id: newID,
    details: {},
    source_type: "link",
    source_id: streamCard.id,
    url: streamCard.url,
    starts_at: insertAtUnix,
    finishes_at: -1
  };

  if (elements.length === 0) {
    elements.push(newElement);
    return { manifest: next, elementID: newID };
  }

  const containingIndex = elements.findIndex((element) => {
    if (element.finishes_at === -1) {
      return insertAtUnix > element.starts_at;
    }
    return insertAtUnix > element.starts_at && insertAtUnix < element.finishes_at;
  });
  if (containingIndex >= 0) {
    const current = { ...elements[containingIndex] };
    newElement.finishes_at = current.finishes_at;
    current.finishes_at = insertAtUnix;
    elements[containingIndex] = current;
    elements.splice(containingIndex + 1, 0, newElement);
    return { manifest: next, elementID: newID };
  }

  const first = elements[0];
  if (insertAtUnix < first.starts_at) {
    newElement.finishes_at = first.starts_at;
    elements.unshift(newElement);
    return { manifest: next, elementID: newID };
  }

  const last = elements[elements.length - 1];
  if (last.finishes_at !== -1 && insertAtUnix >= last.finishes_at) {
    newElement.starts_at = Math.max(insertAtUnix, last.finishes_at);
    elements.push(newElement);
    return { manifest: next, elementID: newID };
  }

  throw new Error("Choose a free point inside the current timeline to split or append an element.");
}

function buildDefaultManifest(channelID, streamCard, startsAt) {
  return {
    version: "1.0",
    channel_id: channelID,
    scenes: [
      {
        id: "scene_1",
        details: [],
        slots: [
          {
            details: {},
            elements: [
              {
                id: `${streamCard.id}-${startsAt}`,
                details: {},
                source_type: "link",
                source_id: streamCard.id,
                url: streamCard.url,
                starts_at: startsAt,
                finishes_at: -1
              }
            ]
          }
        ]
      }
    ]
  };
}

function cloneManifest(manifest) {
  return JSON.parse(JSON.stringify(manifest));
}

function manifestsEqual(left, right) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function inferStreamKind(element) {
  const source = `${element.source_id ?? ""} ${element.url ?? ""}`.toLowerCase();
  if (source.includes("camera") || source.includes("guest")) {
    return "camera";
  }
  return "screen";
}

function getTimelineBaseDate(manifest, fallbackDate) {
  const firstElement = getManifestElements(manifest)[0];
  const date = firstElement ? new Date(firstElement.starts_at * 1000) : new Date(fallbackDate);
  const start = new Date(date);
  start.setHours(0, 0, 0, 0);
  return start;
}

function timelinePercentToUnix(percent, baseDate) {
  const start = new Date(baseDate);
  start.setHours(0, 0, 0, 0);
  return Math.round(start.getTime() / 1000 + (clamp(percent, 0, 100) / 100) * 86400);
}

function unixToTimelinePercent(unixSeconds, baseDate) {
  const start = new Date(baseDate);
  start.setHours(0, 0, 0, 0);
  return clamp(((unixSeconds * 1000 - start.getTime()) / 86400000) * 100, 0, 100);
}

function visiblePercentToUnix(percent, viewStartUnix, visibleRangeSeconds) {
  return Math.round(viewStartUnix + (clamp(percent, 0, 100) / 100) * visibleRangeSeconds);
}

function unixToVisiblePercent(unixSeconds, viewStartUnix, visibleRangeSeconds) {
  return clamp(((unixSeconds - viewStartUnix) / visibleRangeSeconds) * 100, 0, 100);
}

function normalizeViewStartUnix(candidateStartUnix, dayStartUnix, visibleRangeSeconds) {
  const maxStart = dayStartUnix + 86400 - visibleRangeSeconds;
  return Math.round(clamp(candidateStartUnix, dayStartUnix, maxStart));
}

function clampZoomHours(value) {
  const options = [24, 12, 6, 3, 1];
  return options.reduce((closest, current) =>
    Math.abs(current - value) < Math.abs(closest - value) ? current : closest
  );
}

function buildHourMarksForView(viewStartUnix, visibleRangeSeconds) {
  const stepCount = 12;
  const marks = [];
  for (let index = 0; index < stepCount; index += 1) {
    const unix = Math.round(viewStartUnix + (index / stepCount) * visibleRangeSeconds);
    marks.push({
      key: `${unix}-${index}`,
      label: new Intl.DateTimeFormat(undefined, {
        hour: "numeric",
        minute: visibleRangeSeconds <= 6 * 3600 ? "2-digit" : undefined
      }).format(new Date(unix * 1000))
    });
  }
  return marks;
}

function formatZoomLabel(hours) {
  return hours >= 24 ? "24h view" : `${hours}h view`;
}

function nowUnix(date) {
  return Math.round(date.getTime() / 1000);
}

function parseLocalDateTimeInput(value) {
  const parsed = new Date(value);
  const unix = Math.round(parsed.getTime() / 1000);
  if (!Number.isFinite(unix) || unix <= 0) {
    throw new Error("Invalid local date/time value");
  }
  return unix;
}

function formatLocalDateTimeInput(unixSeconds) {
  const date = new Date(unixSeconds * 1000);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
}

function formatLocalTime(date) {
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit"
  }).format(date);
}

function formatLocalDate(date) {
  return new Intl.DateTimeFormat(undefined, {
    month: "long",
    day: "numeric"
  }).format(date);
}

function formatLocalZone(date) {
  const formatter = new Intl.DateTimeFormat(undefined, {
    timeZoneName: "short"
  });
  const part = formatter.formatToParts(date).find((item) => item.type === "timeZoneName");
  return part?.value ?? Intl.DateTimeFormat().resolvedOptions().timeZone;
}

function formatHourMark(hour) {
  const labelDate = new Date(2000, 0, 1, hour, 0, 0);
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric"
  }).format(labelDate);
}

function getLocalDayProgress(date) {
  const secondsIntoDay = date.getHours() * 3600 + date.getMinutes() * 60 + date.getSeconds();
  return (secondsIntoDay / 86400) * 100;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function clampInt(value, min, max) {
  return Math.round(clamp(value, min, max));
}

function findStreamCard(streams, kindOrID) {
  return streams.find((stream) => stream.id === kindOrID || stream.kind === kindOrID) ?? streams[0];
}

function formatTimelinePoint(percent, baseDate) {
  const startOfDay = new Date(baseDate);
  startOfDay.setHours(0, 0, 0, 0);
  const point = new Date(startOfDay.getTime() + (percent / 100) * 24 * 60 * 60 * 1000);
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit"
  }).format(point);
}

function PlusIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 5v14M5 12h14" />
    </svg>
  );
}

function MinusIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 12h14" />
    </svg>
  );
}

function ToolIcon({ type }) {
  switch (type) {
    case "folder":
      return <FolderIcon />;
    case "stream":
      return <ScreenIcon />;
    case "link":
      return <LinkIcon />;
    case "grid":
      return <GridIcon />;
    case "chat":
      return <ChatIcon />;
    case "gear":
      return <GearIcon />;
    default:
      return null;
  }
}

function BellIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M6.5 9.5a5.5 5.5 0 1111 0v2.9l1.7 2.1a1 1 0 01-.78 1.64H5.58a1 1 0 01-.78-1.64l1.7-2.1V9.5zM10 18a2 2 0 004 0" />
    </svg>
  );
}

function CameraIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M7.5 8.5l1.4-2h6.2l1.4 2H19a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2h2.5zM12 17a3.5 3.5 0 100-7 3.5 3.5 0 000 7z" />
    </svg>
  );
}

function ScreenIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 6.5A1.5 1.5 0 015.5 5h13A1.5 1.5 0 0120 6.5v8A1.5 1.5 0 0118.5 16H13v2h2.5a1 1 0 110 2h-7a1 1 0 010-2H11v-2H5.5A1.5 1.5 0 014 14.5v-8z" />
    </svg>
  );
}

function ImageIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 6a2 2 0 00-2 2v8a2 2 0 002 2h14a2 2 0 002-2V8a2 2 0 00-2-2H5zm3 3a1.5 1.5 0 110 3 1.5 1.5 0 010-3zm11 8H5l3.6-4.2 2.5 2.8 3.2-3.8L19 17z" />
    </svg>
  );
}

function DotsIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 5.5a1.5 1.5 0 110 3 1.5 1.5 0 010-3zm0 5a1.5 1.5 0 110 3 1.5 1.5 0 010-3zm0 5a1.5 1.5 0 110 3 1.5 1.5 0 010-3z" />
    </svg>
  );
}

function CopyIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M9 9.5A1.5 1.5 0 0110.5 8h8A1.5 1.5 0 0120 9.5v8a1.5 1.5 0 01-1.5 1.5h-8A1.5 1.5 0 019 17.5v-8zM6.5 5A1.5 1.5 0 005 6.5v8A1.5 1.5 0 006.5 16H7V9.5A2.5 2.5 0 019.5 7H16v-.5A1.5 1.5 0 0014.5 5h-8z" />
    </svg>
  );
}

function FolderIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 7.5A1.5 1.5 0 015.5 6H10l1.6 1.5H18.5A1.5 1.5 0 0120 9v8.5a1.5 1.5 0 01-1.5 1.5h-13A1.5 1.5 0 014 17.5v-10z" />
    </svg>
  );
}

function LinkIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M13.4 10.6l-2.8 2.8m-2.1 3.5l-1.4 1.4a3 3 0 11-4.2-4.2l3.5-3.5a3 3 0 014.2 0m5.9-3.6l1.4-1.4a3 3 0 114.2 4.2l-3.5 3.5a3 3 0 01-4.2 0" />
    </svg>
  );
}

function GridIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 5h5v5H5V5zm9 0h5v5h-5V5zM5 14h5v5H5v-5zm9 0h5v5h-5v-5z" />
    </svg>
  );
}

function ChatIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5.5 6h13A1.5 1.5 0 0120 7.5v7a1.5 1.5 0 01-1.5 1.5H11l-4 3v-3H5.5A1.5 1.5 0 014 14.5v-7A1.5 1.5 0 015.5 6z" />
    </svg>
  );
}

function GearIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M11 3h2l.5 2.1a7.8 7.8 0 011.7.7l1.9-1.1 1.4 1.4-1.1 1.9c.3.5.5 1.1.7 1.7L21 11v2l-2.1.5a7.8 7.8 0 01-.7 1.7l1.1 1.9-1.4 1.4-1.9-1.1a7.8 7.8 0 01-1.7.7L13 21h-2l-.5-2.1a7.8 7.8 0 01-1.7-.7l-1.9 1.1-1.4-1.4 1.1-1.9a7.8 7.8 0 01-.7-1.7L3 13v-2l2.1-.5c.2-.6.4-1.2.7-1.7L4.7 6.9 6.1 5.5 8 6.6c.5-.3 1.1-.5 1.7-.7L11 3zm1 6a3 3 0 100 6 3 3 0 000-6z" />
    </svg>
  );
}

function EyeIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 7c5.2 0 8.8 5 8.8 5s-3.6 5-8.8 5-8.8-5-8.8-5S6.8 7 12 7zm0 2.5a2.5 2.5 0 100 5 2.5 2.5 0 000-5z" />
    </svg>
  );
}

function MicIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 15a3 3 0 003-3V7a3 3 0 10-6 0v5a3 3 0 003 3zm5-3a5 5 0 01-10 0H5a7 7 0 006 6.9V21h2v-2.1A7 7 0 0019 12h-2z" />
    </svg>
  );
}

function UsersIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M9 12a3 3 0 100-6 3 3 0 000 6zm6 1a2.5 2.5 0 100-5 2.5 2.5 0 000 5zM4 18a4.5 4.5 0 019 0v1H4v-1zm10 1v-1a5.8 5.8 0 00-1.1-3.4A4.5 4.5 0 0120 18v1h-6z" />
    </svg>
  );
}

function ArrowLeftIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M14.5 6l-6 6 6 6" />
    </svg>
  );
}

function ArrowRightIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M9.5 6l6 6-6 6" />
    </svg>
  );
}

function CloseIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M6 6l12 12M18 6L6 18" />
    </svg>
  );
}

export default App;
