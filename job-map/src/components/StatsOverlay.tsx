import { useEffect, useState } from 'react';
import clsx from 'clsx';

interface StatsOverlayProps {
  totalJobs: number;
  displayedJobs: number;
  totalLocations: number;
  popupOpen?: boolean;
}

export function StatsOverlay({ totalJobs, displayedJobs, totalLocations, popupOpen = false }: StatsOverlayProps) {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth <= 768);
    };

    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  // Hide on mobile when popup is open to prevent overlap
  const shouldHide = isMobile && popupOpen;

  return (
    <div
      className={clsx(
        'stats-overlay',
        'absolute z-1',
        'bg-black/50 backdrop-blur-[20px]',
        'border border-white/10 rounded-2xl',
        'text-white font-[system-ui,-apple-system,BlinkMacSystemFont,"Inter",sans-serif]',
        'transition-[opacity,transform] duration-200 ease-in-out',
        'top-6 left-6 px-6 py-5 min-w-[200px]',
        'md:top-6 md:left-6 md:px-6 md:py-5 md:min-w-[200px]',
        'max-md:top-3 max-md:left-3 max-md:px-5 max-md:py-4 max-md:min-w-[160px]',
        {
          'opacity-0 pointer-events-none -translate-y-[10px]': shouldHide,
          'opacity-100 pointer-events-auto translate-y-0': !shouldHide,
        }
      )}
    >
      <div className="flex flex-col gap-3 text-[13px] tracking-[0.01em]">
        <div className="stats-number text-[32px] md:text-[32px] max-md:text-2xl font-light text-white leading-none tabular-nums">
          {displayedJobs.toLocaleString()}
        </div>

        <div className="flex gap-4 text-xs text-white/60">
          <div>
            <div className="tabular-nums">
              {totalLocations.toLocaleString()}
            </div>
            <div className="text-[10px] text-white/40">
              locations
            </div>
          </div>
          <div className="w-px bg-white/10" />
          <div>
            <div className="tabular-nums">
              {totalJobs.toLocaleString()}
            </div>
            <div className="text-[10px] text-white/40">
              total
            </div>
          </div>
        </div>
      </div>
      <div
        className={clsx(
          'contribution-inline',
          'mt-[14px] pt-[10px] border-t border-white/8',
          'flex flex-wrap items-center justify-between gap-[10px]',
          'text-[11px] text-white/65',
          'max-md:flex-col max-md:items-start max-md:gap-2 max-md:mt-[10px] max-md:pt-2'
        )}
      >
        <div className="flex gap-2 flex-wrap max-md:w-full">
          <a
            href="https://github.com/stapply-ai/jobs"
            target="_blank"
            rel="noopener noreferrer"
            className={clsx(
              'text-white no-underline',
              'bg-white/8 px-[10px] py-1 rounded-full',
              'border border-white/12',
              'text-[11px] inline-flex items-center gap-1.5',
              'transition-[border-color,background-color] duration-200 ease-in-out',
              'max-md:w-full max-md:justify-center'
            )}
          >
            Contribute jobs
          </a>
          <a
            href="https://github.com/stapply-ai/jobs"
            target="_blank"
            rel="noopener noreferrer"
            className={clsx(
              'text-white no-underline',
              'bg-white/8 px-[10px] py-1 rounded-full',
              'border border-white/12',
              'text-[11px] inline-flex items-center gap-1.5',
              'transition-[border-color,background-color] duration-200 ease-in-out',
              'max-md:w-full max-md:justify-center'
            )}
          >
            Star the repo
          </a>
        </div>
      </div>
    </div>
  );
}
