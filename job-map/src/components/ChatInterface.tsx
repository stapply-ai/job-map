import { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import clsx from 'clsx';
import { AIService } from '../services/aiService';
import type { FunctionCallResult } from '../services/aiService';

const StapplyLogo = ({ size = 36 }: { size?: number }) => (
    <svg
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill="currentColor"
        role="img"
        aria-label="Stapply logo - stacked documents icon"
        className="block"
    >
        <rect x="3" y="6" width="14" height="16" rx="2" fill="#3b82f6" opacity="0.3"></rect>
        <rect x="4" y="4" width="14" height="16" rx="2" fill="#3b82f6" opacity="0.8"></rect>
        <rect x="5" y="2" width="14" height="16" rx="2" fill="#2563eb" opacity="0.9"></rect>
        <rect x="7" y="4" width="10" height="3" rx="1" fill="white"></rect>
        <line x1="7" y1="9" x2="17" y2="9" strokeWidth="0.5" stroke="white" opacity="0.6"></line>
        <line x1="7" y1="11" x2="15" y2="11" strokeWidth="0.5" stroke="white" opacity="0.6"></line>
        <line x1="7" y1="13" x2="16" y2="13" strokeWidth="0.5" stroke="white" opacity="0.6"></line>
    </svg>
);

interface ChatInterfaceProps {
    aiService: AIService;
    onClose?: () => void;
}

interface Message {
    id: string;
    role: 'user' | 'assistant';
    content: string;
    functionCalls?: FunctionCallResult[];
    isLoading?: boolean;
}

export function ChatInterface({ aiService, onClose }: ChatInterfaceProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState<Message[]>([]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    useEffect(() => {
        if (isOpen && inputRef.current) {
            inputRef.current.focus();
        }
    }, [isOpen]);

    const handleSend = async () => {
        if (!input.trim() || isLoading) return;

        const userMessage: Message = {
            id: Date.now().toString(),
            role: 'user',
            content: input.trim(),
        };

        setMessages(prev => [...prev, userMessage]);
        setInput('');
        setIsLoading(true);

        const loadingMessage: Message = {
            id: (Date.now() + 1).toString(),
            role: 'assistant',
            content: '',
            isLoading: true,
        };
        setMessages(prev => [...prev, loadingMessage]);

        try {
            const response = await aiService.sendMessage(userMessage.content);

            // Remove loading message and add actual response
            setMessages(prev => {
                const filtered = prev.filter(msg => msg.id !== loadingMessage.id);
                return [
                    ...filtered,
                    {
                        id: Date.now().toString(),
                        role: 'assistant',
                        content: response.response,
                        functionCalls: response.functionCalls,
                    },
                ];
            });
        } catch (error) {
            // Remove loading message and add error
            setMessages(prev => {
                const filtered = prev.filter(msg => msg.id !== loadingMessage.id);
                return [
                    ...filtered,
                    {
                        id: Date.now().toString(),
                        role: 'assistant',
                        content: `Sorry, I encountered an error: ${error instanceof Error ? error.message : 'Unknown error'}`,
                    },
                ];
            });
        } finally {
            setIsLoading(false);
        }
    };

    const handleKeyPress = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    if (!isOpen) {
        return (
            <button
                onClick={() => setIsOpen(true)}
                className={clsx(
                    'fixed bottom-6 right-6 z-1000',
                    'w-16 h-16 rounded-full',
                    'bg-blue-500 border-none cursor-pointer',
                    'shadow-[0_4px_12px_rgba(59,130,246,0.4)]',
                    'flex items-center justify-center',
                    'transition-all duration-200',
                    'p-0 overflow-hidden',
                    'hover:scale-110 hover:shadow-[0_6px_16px_rgba(59,130,246,0.6)]'
                )}
            >
                <StapplyLogo size={40} />
            </button>
        );
    }

    return (
        <div
            className={clsx(
                'fixed bottom-6 right-6 z-1000',
                'w-[400px] max-w-[calc(100vw-48px)]',
                'h-[580px] max-h-[calc(100vh-48px)]',
                'bg-black backdrop-blur-2xl',
                'border border-white/10 rounded-2xl',
                'flex flex-col',
                'shadow-[0_8px_32px_rgba(0,0,0,0.8)]',
                'font-[system-ui,-apple-system,BlinkMacSystemFont,"Inter",sans-serif]',
                'overflow-hidden'
            )}
        >
            {/* Header */}
            <div className="px-5 py-4 border-b border-white/10 flex items-center justify-between bg-black/30">
                <div className="flex items-center gap-2.5">
                    <StapplyLogo size={24} />
                    <h3 className="m-0 text-[15px] font-medium text-white tracking-[-0.01em]">
                        Assistant
                    </h3>
                </div>
                <button
                    onClick={() => {
                        setIsOpen(false);
                        onClose?.();
                    }}
                    className={clsx(
                        'bg-transparent border-none rounded-md',
                        'w-6 h-6 flex items-center justify-center',
                        'cursor-pointer text-white/40 text-xl leading-none',
                        'transition-all duration-150',
                        'hover:bg-white/10 hover:text-white/80'
                    )}
                >
                    ×
                </button>
            </div>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-4 bg-black">
                {messages.length === 0 && (
                    <div className="text-white/50 text-[13px] text-center py-10 px-5 flex flex-col items-center gap-3">
                        <StapplyLogo size={32} />
                        <p className="m-0 font-normal text-white/60 leading-normal">
                            Ask about jobs or navigate the map
                        </p>
                        <div className="mt-1 text-[11px] text-white/40 leading-relaxed">
                            "Show jobs in San Francisco"<br />
                            "Zoom to New York"
                        </div>
                    </div>
                )}

                {messages.map((message) => (
                    <div
                        key={message.id}
                        className={clsx(
                            'flex flex-col gap-2',
                            message.role === 'user' ? 'items-end' : 'items-start'
                        )}
                    >
                        <div
                            className={clsx(
                                'max-w-[85%] px-3.5 py-2.5',
                                'text-white text-[13px] leading-normal wrap-break-word',
                                message.role === 'user'
                                    ? 'bg-blue-500 rounded-[12px_4px_12px_12px]'
                                    : 'bg-white/8 rounded-[4px_12px_12px_12px]'
                            )}
                        >
                            {message.isLoading ? (
                                <div className="flex items-center gap-2">
                                    <div className="w-2.5 h-2.5 border-[1.5px] border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
                                    <span className="text-white/50 text-xs">Thinking...</span>
                                </div>
                            ) : (
                                typeof message.content === 'string' ? (
                                    <div className="markdown-content">
                                        <ReactMarkdown
                                            components={{
                                                // Headings
                                                h1: ({ node, ...props }) => (
                                                    <h1 className="text-lg font-semibold m-0 mb-3 text-white leading-snug" {...props} />
                                                ),
                                                h2: ({ node, ...props }) => (
                                                    <h2 className="text-base font-semibold m-0 mb-2.5 text-white leading-snug" {...props} />
                                                ),
                                                h3: ({ node, ...props }) => (
                                                    <h3 className="text-[15px] font-semibold m-0 mb-2 text-white leading-snug" {...props} />
                                                ),
                                                // Paragraphs
                                                p: ({ node, ...props }) => (
                                                    <p className="m-0 mb-3 text-white leading-relaxed text-[13px]" {...props} />
                                                ),
                                                // Lists
                                                ul: ({ node, ...props }) => (
                                                    <ul className="m-0 mb-3 pl-5 text-white text-[13px] leading-relaxed" {...props} />
                                                ),
                                                ol: ({ node, ...props }) => (
                                                    <ol className="m-0 mb-3 pl-5 text-white text-[13px] leading-relaxed" {...props} />
                                                ),
                                                li: ({ node, ...props }) => (
                                                    <li className="m-0 mb-1.5 text-white leading-relaxed" {...props} />
                                                ),
                                                // Links
                                                a: ({ node, ...props }) => (
                                                    <a
                                                        {...props}
                                                        target="_blank"
                                                        rel="noopener noreferrer"
                                                        className="text-blue-500 underline decoration-blue-500/40 transition-all duration-200 hover:text-blue-400 hover:decoration-blue-400"
                                                    />
                                                ),
                                                // Code blocks
                                                code: ({ node, inline, ...props }: any) => {
                                                    if (inline) {
                                                        return (
                                                            <code
                                                                className="bg-blue-500/15 text-blue-300 px-1.5 py-0.5 rounded text-xs font-mono"
                                                                {...props}
                                                            />
                                                        );
                                                    }
                                                    return (
                                                        <code
                                                            className="block bg-black/40 text-gray-200 p-3 rounded-lg text-xs font-mono overflow-auto m-0 mb-3 border border-white/10"
                                                            {...props}
                                                        />
                                                    );
                                                },
                                                // Pre blocks
                                                pre: ({ node, ...props }) => (
                                                    <pre className="m-0 mb-3 overflow-auto" {...props} />
                                                ),
                                                // Blockquotes
                                                blockquote: ({ node, ...props }) => (
                                                    <blockquote
                                                        className="m-0 mb-3 pl-4 border-l-[3px] border-blue-500/50 text-white/80 italic"
                                                        {...props}
                                                    />
                                                ),
                                                // Strong/Bold
                                                strong: ({ node, ...props }) => (
                                                    <strong className="font-semibold text-white" {...props} />
                                                ),
                                                // Emphasis/Italic
                                                em: ({ node, ...props }) => (
                                                    <em className="italic text-white" {...props} />
                                                ),
                                                // Horizontal rule
                                                hr: ({ node, ...props }) => (
                                                    <hr className="border-none border-t border-white/10 my-4" {...props} />
                                                ),
                                            }}
                                        >
                                            {message.content}
                                        </ReactMarkdown>
                                    </div>
                                ) : (
                                    JSON.stringify(message.content)
                                )
                            )}
                        </div>

                        {message.functionCalls && message.functionCalls.length > 0 && (
                            <div className="text-[10px] text-white/50 px-2 py-1 bg-blue-500/15 rounded-md mt-1">
                                {message.functionCalls.map((fc, idx) => (
                                    <div key={idx}>
                                        {fc.name}: {fc.result.message}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                ))}

                <div ref={messagesEndRef} />
            </div>

            {/* Input */}
            <div className="px-4 py-3.5 border-t border-white/10 bg-black/30">
                <div
                    className={clsx(
                        'flex gap-0 items-stretch',
                        'bg-white/8 rounded-xl border border-white/12 overflow-hidden',
                        'transition-all duration-200',
                        'focus-within:border-blue-500/50 focus-within:bg-white/10'
                    )}
                >
                    <input
                        ref={inputRef}
                        type="text"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyPress={handleKeyPress}
                        placeholder="Message..."
                        disabled={isLoading}
                        className={clsx(
                            'flex-1 px-4 py-2.5',
                            'bg-transparent border-none text-white text-[13px] outline-none',
                            'font-inherit placeholder:text-white/40'
                        )}
                    />
                    <div className="w-px bg-white/8 my-2" />
                    <button
                        onClick={handleSend}
                        disabled={!input.trim() || isLoading}
                        className={clsx(
                            'px-4 bg-transparent border-none',
                            'text-[13px] font-medium',
                            'transition-all duration-150',
                            'flex items-center justify-center min-w-[60px]',
                            isLoading || !input.trim()
                                ? 'text-white/25 cursor-not-allowed'
                                : 'text-blue-500 cursor-pointer hover:text-blue-600'
                        )}
                    >
                        {isLoading ? (
                            <div className="w-3.5 h-3.5 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-[spin_0.7s_linear_infinite]" />
                        ) : (
                            'Send'
                        )}
                    </button>
                </div>
            </div>
        </div>
    );
}
