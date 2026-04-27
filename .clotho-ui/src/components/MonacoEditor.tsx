import { onMount, onCleanup, createEffect } from 'solid-js';
import * as monaco from 'monaco-editor';

interface MonacoEditorProps {
  content: string;
  language?: string;
  readOnly?: boolean;
  onChange?: (value: string) => void;
  filePath?: string | null;
}

export default function MonacoEditor(props: MonacoEditorProps) {
  let editorRef: HTMLDivElement | undefined;
  let editor: monaco.editor.IStandaloneCodeEditor | undefined;

  onMount(() => {
    if (!editorRef) return;

    // Configure Rust language support
    monaco.languages.register({ id: 'rust' });
    
    // Set Rust syntax highlighting
    monaco.languages.setMonarchTokensProvider('rust', {
      tokenizer: {
        root: [
          [/\b(fn|let|mut|const|struct|enum|impl|trait|type|use|mod|pub|crate|self|super)\b/, 'keyword'],
          [/\b(if|else|match|loop|while|for|in|break|continue|return|yield|async|await)\b/, 'keyword'],
          [/\b(true|false|Some|None|Ok|Err)\b/, 'keyword'],
          [/\b(i8|i16|i32|i64|i128|u8|u16|u32|u64|u128|f32|f64|bool|char|str|String|Vec|Option|Result)\b/, 'type'],
          [/\b(Self)\b/, 'type'],
          [/\b(print|println|eprint|eprintln|format|panic|todo|unimplemented|unreachable)\b/, 'function'],
          [/\b(derive|cfg|allow|warn|deny|forbid|macro_rules)\b/, 'attribute'],
          [/#\[.*\]/, 'attribute'],
          [/\/\/.*$/, 'comment'],
          [/\/\*[\s\S]*?\*\//, 'comment'],
          [/"([^"\\]|\\.)*$/, 'string.invalid'],
          [/"/, 'string', '@string'],
          [/'[^\\']'/, 'string'],
          [/(')(@escapes)(')/, ['string', 'string', 'string']],
          [/'/, 'string.invalid'],
          [/\d*\.\d+([eE][\-+]?\d+)?/, 'number.float'],
          [/0[xX][0-9a-fA-F]+/, 'number.hex'],
          [/0[oO][0-7]+/, 'number.octal'],
          [/0[bB][1]+/, 'number.binary'],
          [/\d+/, 'number'],
          [/[;,.]/, 'delimiter'],
          [/[{}()\[\]]/, '@brackets'],
          [/[<>](?!@symbols)/, '@brackets'],
          [/@symbols/, 'delimiter'],
          [/[a-zA-Z_]\w*/, 'identifier'],
        ],
        string: [
          [/[^\\"]+/, 'string'],
          [/\\./, 'string.escape'],
          [/"/, 'string', '@pop']
        ],
      },
      escapes: /\\[nrt\\0]/,
      symbols: /[=><!~?:&|+\-*\/\^%]+/,
    });

    // Create editor instance
    editor = monaco.editor.create(editorRef, {
      value: props.content,
      language: props.language || 'rust',
      theme: 'vs-dark',
      readOnly: props.readOnly ?? false,
      automaticLayout: true,
      minimap: { enabled: true },
      scrollBeyondLastLine: false,
      fontSize: 14,
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', Consolas, monospace",
      lineNumbers: 'on',
      renderWhitespace: 'selection',
      bracketPairColorization: { enabled: true },
      guides: {
        bracketPairs: true,
        indentation: true,
      },
      suggest: {
        showKeywords: true,
        showSnippets: true,
      },
      quickSuggestions: {
        other: true,
        comments: false,
        strings: false,
      },
    });

    // Handle content changes
    editor.onDidChangeModelContent(() => {
      if (editor && props.onChange) {
        props.onChange(editor.getValue());
      }
    });

    // Add keyboard shortcuts
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
      // Save command - will be handled by parent
      const event = new CustomEvent('monaco-save', { detail: { content: editor?.getValue() } });
      window.dispatchEvent(event);
    });
  });

  // Update content when prop changes
  createEffect(() => {
    if (editor && editor.getValue() !== props.content) {
      editor.setValue(props.content);
    }
  });

  // Update read-only mode
  createEffect(() => {
    if (editor) {
      editor.updateOptions({ readOnly: props.readOnly ?? false });
    }
  });

  // Update language
  createEffect(() => {
    if (editor && props.filePath) {
      const model = editor.getModel();
      if (model) {
        const ext = props.filePath.split('.').pop()?.toLowerCase();
        let language = 'rust';
        
        switch (ext) {
          case 'toml':
            language = 'toml';
            break;
          case 'json':
            language = 'json';
            break;
          case 'yaml':
          case 'yml':
            language = 'yaml';
            break;
          case 'md':
            language = 'markdown';
            break;
          case 'rs':
            language = 'rust';
            break;
        }
        
        monaco.editor.setModelLanguage(model, language);
      }
    }
  });

  onCleanup(() => {
    if (editor) {
      editor.dispose();
    }
  });

  return (
    <div 
      ref={editorRef} 
      style={{ 
        width: '100%', 
        height: '100%',
        border: '1px solid #333'
      }} 
    />
  );
}