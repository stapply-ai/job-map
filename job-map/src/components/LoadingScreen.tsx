import { useEffect } from 'react';

interface LoadingScreenProps {
  stage: string;
  current?: number;
  total?: number;
}

export function LoadingScreen() {


  return (
    <div className="fixed top-0 left-0 w-screen h-screen flex flex-col items-center justify-center bg-black gap-6 z-[9999]">
      <img
        src="/stapply_small.svg"
        alt="Stapply Logo"
        className="w-auto h-auto max-w-[200px] max-h-[100px]"
      />
      <p className="text-base text-white m-0 font-normal">
        loading
      </p>
    </div>
  );
}
