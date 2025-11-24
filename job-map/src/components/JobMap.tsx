import { useState, useCallback, useMemo, useEffect, useImperativeHandle, forwardRef } from 'react';
import Map, { Marker, Popup, NavigationControl, FullscreenControl } from 'react-map-gl/mapbox';
import clsx from 'clsx';
import type { JobMarker } from '../types';
import { StatsOverlay } from './StatsOverlay';
import type { MapControlCallbacks, ViewState } from '../utils/mapControl';
import 'mapbox-gl/dist/mapbox-gl.css';

interface JobMapProps {
  jobs: JobMarker[];
  mapboxToken: string;
  totalJobs?: number;
  isLoadingMore?: boolean;
  loadingProgress?: { current: number; total: number };
  onMapControlReady?: (callbacks: MapControlCallbacks) => void;
  filteredJobs?: JobMarker[] | null;
  onViewStateChange?: (viewState: ViewState) => void;
}

interface ClusterMarker {
  lat: number;
  lng: number;
  jobs: JobMarker[];
  isCluster: boolean;
}

const MAX_ZOOM_FOR_CLUSTERING = 10;

function calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371; // Earth's radius in km
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
    Math.cos((lat2 * Math.PI) / 180) *
    Math.sin(dLng / 2) *
    Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function createClusters(jobs: JobMarker[], zoom: number): ClusterMarker[] {
  if (zoom > MAX_ZOOM_FOR_CLUSTERING) {
    // Don't cluster at high zoom levels
    return jobs.map(job => ({
      lat: job.lat,
      lng: job.lng,
      jobs: [job],
      isCluster: false,
    }));
  }

  const clusters: ClusterMarker[] = [];
  const used = new Set<number>();

  // Simple clustering algorithm based on distance
  const clusterDistanceKm = 50 / Math.pow(2, zoom); // Decrease distance as zoom increases

  jobs.forEach((job, index) => {
    if (used.has(index)) return;

    const cluster: JobMarker[] = [job];
    used.add(index);

    // Find nearby jobs
    jobs.forEach((otherJob, otherIndex) => {
      if (used.has(otherIndex)) return;

      const distance = calculateDistance(job.lat, job.lng, otherJob.lat, otherJob.lng);
      if (distance < clusterDistanceKm) {
        cluster.push(otherJob);
        used.add(otherIndex);
      }
    });

    // Calculate cluster center
    const centerLat = cluster.reduce((sum, j) => sum + j.lat, 0) / cluster.length;
    const centerLng = cluster.reduce((sum, j) => sum + j.lng, 0) / cluster.length;

    clusters.push({
      lat: centerLat,
      lng: centerLng,
      jobs: cluster,
      isCluster: cluster.length > 1,
    });
  });

  return clusters;
}

export const JobMap = forwardRef<MapControlCallbacks, JobMapProps>(
  ({ jobs, mapboxToken, totalJobs, isLoadingMore, loadingProgress, onMapControlReady, filteredJobs, onViewStateChange }, ref) => {
    // Calculate initial view state based on jobs if available
    const getInitialViewState = (): ViewState => {
      if (jobs.length > 0) {
        // Calculate center of all jobs
        const validJobs = jobs.filter(j => !isNaN(j.lat) && !isNaN(j.lng));
        if (validJobs.length > 0) {
          const avgLat = validJobs.reduce((sum, j) => sum + j.lat, 0) / validJobs.length;
          const avgLng = validJobs.reduce((sum, j) => sum + j.lng, 0) / validJobs.length;
          return {
            longitude: avgLng,
            latitude: avgLat,
            zoom: 2,
          };
        }
      }
      return {
        longitude: -95.7129,
        latitude: 37.0902,
        zoom: 3.5,
      };
    };

    const [viewState, setViewState] = useState<ViewState>(getInitialViewState());

    // Update view state when jobs change (if initially empty)
    useEffect(() => {
      if (jobs.length > 0 && viewState.longitude === -95.7129 && viewState.latitude === 37.0902) {
        const newViewState = getInitialViewState();
        setViewState(newViewState);
      }
    }, [jobs.length]);

    const [popupJob, setPopupJob] = useState<JobMarker | null>(null);
    const [popupJobsAtLocation, setPopupJobsAtLocation] = useState<JobMarker[]>([]);
    const [currentJobIndex, setCurrentJobIndex] = useState(0);

    // Use filtered jobs if provided, otherwise use all jobs
    const displayJobs = useMemo(() => {
      return filteredJobs !== null && filteredJobs !== undefined ? filteredJobs : jobs;
    }, [jobs, filteredJobs]);

    const clusters = useMemo(() => {
      console.log(`Creating clusters from ${displayJobs.length} jobs`);
      const result = createClusters(displayJobs, viewState.zoom);
      console.log(`Created ${result.length} clusters`);
      return result;
    }, [displayJobs, viewState.zoom]);

    const uniqueLocations = useMemo(() => {
      const locations = new Set(displayJobs.map(job => job.location));
      return locations.size;
    }, [displayJobs]);

    // Map control callbacks
    const flyTo = useCallback((longitude: number, latitude: number, zoom?: number) => {
      setViewState(prev => ({
        ...prev,
        longitude,
        latitude,
        zoom: zoom !== undefined ? zoom : prev.zoom,
      }));
    }, []);

    const setZoom = useCallback((zoom: number) => {
      setViewState(prev => ({
        ...prev,
        zoom: Math.max(1, Math.min(15, zoom)),
      }));
    }, []);

    const setFilteredJobs = useCallback((_filtered: JobMarker[] | null) => {
      // This is handled by the filteredJobs prop from parent
      // We could emit an event here if needed
    }, []);

    const getViewState = useCallback((): ViewState => {
      return { ...viewState };
    }, [viewState]);

    // Expose callbacks via ref and callback
    const callbacks: MapControlCallbacks = useMemo(() => ({
      flyTo,
      setZoom,
      setFilteredJobs,
      getViewState,
    }), [flyTo, setZoom, setFilteredJobs, getViewState]);

    useEffect(() => {
      onMapControlReady?.(callbacks);
    }, [callbacks, onMapControlReady]);

    useImperativeHandle(ref, () => callbacks, [callbacks]);

    // Find all jobs at the same location (within a very small distance)
    const findJobsAtLocation = useCallback((lat: number, lng: number, allJobs: JobMarker[]): JobMarker[] => {
      const TOLERANCE_KM = 0.001; // ~100 meters - very small tolerance for "same location"
      return allJobs.filter(job => {
        const distance = calculateDistance(lat, lng, job.lat, job.lng);
        return distance < TOLERANCE_KM;
      });
    }, []);

    const handleClusterClick = useCallback((cluster: ClusterMarker) => {
      if (cluster.isCluster && cluster.jobs.length > 1) {
        // Check if we're zoomed in enough - if so, show popup instead of zooming
        if (viewState.zoom >= MAX_ZOOM_FOR_CLUSTERING) {
          // At high zoom, show popup with navigation
          const jobsAtLocation = findJobsAtLocation(cluster.lat, cluster.lng, displayJobs);
          setPopupJobsAtLocation(jobsAtLocation);
          setCurrentJobIndex(0);
          setPopupJob(jobsAtLocation[0]);
        } else {
          // Zoom in on cluster
          setViewState(prev => ({
            ...prev,
            longitude: cluster.lng,
            latitude: cluster.lat,
            zoom: Math.min(prev.zoom + 2, 15),
          }));
        }
      } else {
        // Single marker clicked - check if there are other jobs at the same location
        const jobsAtLocation = findJobsAtLocation(cluster.lat, cluster.lng, displayJobs);
        setPopupJobsAtLocation(jobsAtLocation);
        setCurrentJobIndex(0);
        setPopupJob(jobsAtLocation[0]);
      }
    }, [viewState.zoom, displayJobs, findJobsAtLocation]);

    const handleNextJob = useCallback(() => {
      const nextIndex = (currentJobIndex + 1) % popupJobsAtLocation.length;
      setCurrentJobIndex(nextIndex);
      setPopupJob(popupJobsAtLocation[nextIndex]);
    }, [currentJobIndex, popupJobsAtLocation]);

    const handlePrevJob = useCallback(() => {
      const prevIndex = currentJobIndex === 0 ? popupJobsAtLocation.length - 1 : currentJobIndex - 1;
      setCurrentJobIndex(prevIndex);
      setPopupJob(popupJobsAtLocation[prevIndex]);
    }, [currentJobIndex, popupJobsAtLocation]);

    const handleClosePopup = useCallback(() => {
      setPopupJob(null);
      setPopupJobsAtLocation([]);
      setCurrentJobIndex(0);
    }, []);

    // Keyboard navigation support
    useEffect(() => {
      if (!popupJob || popupJobsAtLocation.length <= 1) return;

      const handleKeyPress = (e: KeyboardEvent) => {
        if (e.key === 'ArrowLeft') {
          e.preventDefault();
          handlePrevJob();
        } else if (e.key === 'ArrowRight') {
          e.preventDefault();
          handleNextJob();
        } else if (e.key === 'Escape') {
          e.preventDefault();
          handleClosePopup();
        }
      };

      window.addEventListener('keydown', handleKeyPress);
      return () => {
        window.removeEventListener('keydown', handleKeyPress);
      };
    }, [popupJob, popupJobsAtLocation, handleNextJob, handlePrevJob, handleClosePopup]);

    const getClusterColor = (count: number): string => {
      if (count < 5) return '#3b82f6'; // blue-500
      if (count < 20) return '#00FFB3'; // green
      if (count < 50) return '#FFD600'; // yellow
      if (count < 100) return '#FF6B00'; // orange
      return '#FF0080'; // pink
    };

    const getClusterSize = (count: number): number => {
      if (count < 5) return 32;
      if (count < 20) return 40;
      if (count < 50) return 48;
      if (count < 100) return 56;
      return 64;
    };

    // Ensure map always renders, even with 0 jobs
    if (!mapboxToken) {
      return (
        <div className="w-screen h-screen flex items-center justify-center bg-black text-white">
          <div className="text-center">
            <h2>Mapbox token missing</h2>
            <p>Please set VITE_MAPBOX_TOKEN in your .env file</p>
          </div>
        </div>
      );
    }

    console.log('JobMap render:', {
      jobsCount: jobs.length,
      displayJobsCount: displayJobs.length,
      clustersCount: clusters.length,
      mapboxToken: mapboxToken ? 'present' : 'missing'
    });

    return (
      <div className="w-screen h-screen relative">
        <StatsOverlay
          totalJobs={totalJobs || jobs.length}
          displayedJobs={displayJobs.length}
          totalLocations={uniqueLocations}
          popupOpen={!!popupJob}
        />

        {isLoadingMore && loadingProgress && (
          <div className={clsx(
            'absolute bottom-6 left-1/2 -translate-x-1/2 z-1',
            'bg-black/50 backdrop-blur-[20px]',
            'px-6 py-3.5 rounded-xl',
            'text-white border border-white/10',
            'flex items-center gap-3 min-w-[240px]',
            'font-[system-ui,-apple-system,BlinkMacSystemFont,"Inter",sans-serif]'
          )}>
            <div className="w-3.5 h-3.5 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
            <div className="flex-1">
              <div className="text-xs text-white/90 mb-1.5 font-medium">
                Loading jobs
              </div>
              <div className="w-full h-[3px] bg-blue-500/15 rounded-sm overflow-hidden">
                <div
                  className="h-full bg-blue-500 transition-[width] duration-300 ease-in-out shadow-[0_0_8px_rgba(59,130,246,0.5)]"
                  style={{ width: `${(loadingProgress.current / loadingProgress.total) * 100}%` }}
                />
              </div>
              <div className="text-[10px] text-white/40 mt-1 tabular-nums">
                {loadingProgress.current} / {loadingProgress.total}
              </div>
            </div>
          </div>
        )}

        <Map
          {...viewState}
          onMove={(evt) => {
            const newViewState = evt.viewState;
            setViewState(newViewState);
            onViewStateChange?.(newViewState);
          }}
          onError={(e) => {
            console.error('Map error:', e);
          }}
          onLoad={() => {
            console.log('Map loaded successfully');
          }}
          mapStyle="mapbox://styles/mapbox/dark-v11"
          mapboxAccessToken={mapboxToken}
          style={{ width: '100%', height: '100%' }}
        >
          <NavigationControl position="top-right" />
          <FullscreenControl position="top-right" />

          {clusters.map((cluster, index) => {
            const size = cluster.isCluster ? getClusterSize(cluster.jobs.length) : 20;
            const color = cluster.isCluster ? getClusterColor(cluster.jobs.length) : '#3b82f6';

            return (
              <Marker
                key={`${cluster.lat}-${cluster.lng}-${index}`}
                longitude={cluster.lng}
                latitude={cluster.lat}
                anchor="center"
                onClick={(e) => {
                  e.originalEvent.stopPropagation();
                  handleClusterClick(cluster);
                }}
              >
                {cluster.isCluster ? (
                  <div
                    className="relative cursor-pointer"
                    style={{ width: size, height: size }}
                    onMouseEnter={(e) => {
                      const inner = e.currentTarget.querySelector('.marker-inner') as HTMLElement;
                      if (inner) inner.style.transform = 'scale(1.15)';
                    }}
                    onMouseLeave={(e) => {
                      const inner = e.currentTarget.querySelector('.marker-inner') as HTMLElement;
                      if (inner) inner.style.transform = 'scale(1)';
                    }}
                  >
                    {/* Pulsing ring */}
                    <div
                      className="absolute -inset-2 rounded-full opacity-20 animate-pulse"
                      style={{ backgroundColor: color }}
                    />

                    {/* Main marker */}
                    <div
                      className={clsx(
                        'marker-inner',
                        'relative w-full h-full rounded-full',
                        'bg-black/80 flex items-center justify-center',
                        'font-semibold text-[13px] tabular-nums',
                        'transition-transform duration-200 ease-in-out',
                        'shadow-[0_4px_12px_rgba(0,0,0,0.4)]'
                      )}
                      style={{
                        border: `2px solid ${color}`,
                        color: color,
                        boxShadow: `0 0 20px ${color}40, 0 4px 12px rgba(0, 0, 0, 0.4)`,
                      }}
                    >
                      {cluster.jobs.length}
                    </div>
                  </div>
                ) : (
                  <div
                    className="relative cursor-pointer"
                    onMouseEnter={(e) => {
                      const dot = e.currentTarget.querySelector('.marker-dot') as HTMLElement;
                      if (dot) dot.style.transform = 'scale(1.3)';
                    }}
                    onMouseLeave={(e) => {
                      const dot = e.currentTarget.querySelector('.marker-dot') as HTMLElement;
                      if (dot) dot.style.transform = 'scale(1)';
                    }}
                  >
                    {/* Single job marker - minimalist dot */}
                    <div
                      className="marker-dot rounded-full border-2 border-black/50 transition-transform duration-200 ease-in-out"
                      style={{
                        width: size,
                        height: size,
                        backgroundColor: color,
                        boxShadow: `0 0 12px ${color}80, 0 2px 8px rgba(0, 0, 0, 0.3)`,
                      }}
                    />
                    {/* Inner glow */}
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-2 h-2 rounded-full bg-white opacity-80" />
                  </div>
                )}
              </Marker>
            );
          })}

          {popupJob && (
            <Popup
              longitude={popupJob.lng}
              latitude={popupJob.lat}
              anchor="bottom"
              onClose={handleClosePopup}
              closeButton={false}
              closeOnClick={false}
              offset={15}
              className="custom-popup"
            >
              <div className={clsx(
                'popup-content',
                'bg-black/90 backdrop-blur-[20px]',
                'border border-white/10 rounded-xl p-5',
                'min-w-[300px] max-w-[360px] w-[300px] h-[280px]',
                'text-white font-[system-ui,-apple-system,BlinkMacSystemFont,"Inter",sans-serif]',
                'relative flex flex-col box-border'
              )}>
                {/* Close button */}
                <button
                  onClick={handleClosePopup}
                  className={clsx(
                    'absolute top-3 right-3 z-1',
                    'bg-white/10 border-none rounded-md',
                    'w-7 h-7 flex items-center justify-center cursor-pointer',
                    'text-white/60 text-lg transition-all duration-200',
                    'hover:bg-white/20 hover:text-white'
                  )}
                >
                  ×
                </button>

                {/* Multiple jobs indicator and navigation - always reserve space */}
                <div
                  className={clsx(
                    'flex items-center justify-between mb-3 pr-8 h-8',
                    popupJobsAtLocation.length > 1 ? 'visible' : 'invisible'
                  )}
                >
                  <div className="text-[11px] text-white/50 font-medium">
                    {currentJobIndex + 1} of {popupJobsAtLocation.length} jobs here
                  </div>
                  <div className="flex gap-1">
                    <button
                      onClick={handlePrevJob}
                      title="Previous job (←)"
                      className={clsx(
                        'bg-white/10 border border-white/20 rounded-md',
                        'w-8 h-8 flex items-center justify-center cursor-pointer',
                        'text-white text-base transition-all duration-200',
                        'hover:bg-blue-500/20 hover:border-blue-500'
                      )}
                    >
                      ←
                    </button>
                    <button
                      onClick={handleNextJob}
                      title="Next job (→)"
                      className={clsx(
                        'bg-white/10 border border-white/20 rounded-md',
                        'w-8 h-8 flex items-center justify-center cursor-pointer',
                        'text-white text-base transition-all duration-200',
                        'hover:bg-blue-500/20 hover:border-blue-500'
                      )}
                    >
                      →
                    </button>
                  </div>
                </div>

                <div className="text-[11px] uppercase tracking-wider text-white/40 mb-2 font-medium h-3.5 overflow-hidden text-ellipsis whitespace-nowrap">
                  {popupJob.company}
                </div>

                <h3 className="m-0 mb-4 text-lg font-medium text-white leading-snug pr-5 h-[50px] overflow-hidden line-clamp-2 wrap-break-word">
                  {popupJob.title}
                </h3>

                <div className="text-[13px] text-white/50 mb-5 flex items-center gap-1.5 h-5 overflow-hidden text-ellipsis whitespace-nowrap">
                  <div className="w-1 h-1 rounded-full bg-blue-500 shrink-0" />
                  {popupJob.location}
                </div>

                <a
                  href={popupJob.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={clsx(
                    'flex items-center justify-center gap-2',
                    'px-5 py-3 bg-blue-500 text-black no-underline rounded-lg',
                    'text-[13px] font-semibold border-none w-full tracking-wide',
                    'h-11 shrink-0 transition-all duration-200',
                    'hover:bg-blue-400 hover:-translate-y-px'
                  )}
                >
                  View Job
                  <span className="text-base">→</span>
                </a>
              </div>
            </Popup>
          )}
        </Map>
      </div>
    );
  }
);

JobMap.displayName = 'JobMap';
