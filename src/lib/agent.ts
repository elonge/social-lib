import { GoogleGenerativeAI, FunctionCallingMode } from '@google/generative-ai';

const GEMINI_MODEL = 'gemini-1.5-flash'; 

// --- Tool Adapter ---

function adaptTool(openaiTool: any) {
  // We need to construct the Gemini Tool declaration
  const parameters = JSON.parse(JSON.stringify(openaiTool.parameters));
  
  // Clean up parameters for Gemini
  if (parameters.$schema) delete parameters.$schema;
  if (parameters.additionalProperties !== undefined) delete parameters.additionalProperties;
  
  return {
    declaration: {
      name: openaiTool.name,
      description: openaiTool.description,
      parameters: parameters,
    },
    execute: openaiTool.execute ?? ((args: unknown) => {
      const input = typeof args === 'string' ? args : JSON.stringify(args ?? {});
      return openaiTool.invoke?.(undefined, input);
    })

  };
}

// --- Generic Agent Runner ---

export async function runGeminiAgent<T>(
  apiKey: string,
  systemInstruction: string,
  tools: any[],
  userMessage: string | Array<any>,
  outputToolName: string,
  outputSchema?: any
): Promise<T | null> {
  const genAI = new GoogleGenerativeAI(apiKey);
  const model = genAI.getGenerativeModel({ 
    model: GEMINI_MODEL,
    systemInstruction 
  });

  // 1. Adapt tools
  const adaptedTools = tools.map(adaptTool);
  const toolMap = new Map(adaptedTools.map(t => [t.declaration.name, t.execute]));

  // 2. Add Final Answer Tool
  let allToolDeclarations = [...adaptedTools.map(t => t.declaration)];
  
  if (outputSchema) {
    const finalAnswerToolDeclaration = {
      name: outputToolName,
      description: "Submit the final answer for the user's request. Use this tool when you have gathered all necessary information.",
      parameters: outputSchema
    };
    allToolDeclarations.push(finalAnswerToolDeclaration);
  }

  // 3. Start Chat
  const chat = model.startChat({
    tools: [{ functionDeclarations: allToolDeclarations }],
    toolConfig: outputSchema ? {
      functionCallingConfig: {
        mode: FunctionCallingMode.ANY,
        allowedFunctionNames: [outputToolName, ...adaptedTools.map(t => t.declaration.name)]
      }
    } : undefined
  });

  let response = await chat.sendMessage(userMessage);
  let result: T | null = null;
  const maxTurns = 15; 
  let turns = 0;

  while (turns < maxTurns) {
    const functionCalls = response.response.functionCalls();
    const call = functionCalls?.[0];
    
    if (call) {
      const { name, args } = call;
      console.log(`🤖 [Gemini] Calling tool: ${name}`);

      if (name === outputToolName) {
        result = args as T;
        break;
      }
      const executor = toolMap.get(name);
      if (executor) {
        try {
          const toolResult = await executor(args);
          // Send result back
          response = await chat.sendMessage([{
            functionResponse: {
              name: name,
              response: { result: toolResult } 
            }
          }]);
        } catch (error: any) {
            console.error(`Error executing tool ${name}:`, error);
            response = await chat.sendMessage([{
                functionResponse: {
                    name: name,
                    response: { error: error.message || "Unknown error" }
                }
            }]);
        }
      } else {
        console.error(`Tool ${name} not found`);
        break;
      }
    } else {
      const text = response.response.text();
      console.log(`🤖 [Gemini] Says: ${text}`);
      
      // If we are forced to use tools (Mode.ANY), the model SHOULD have called a tool.
      // If it returns text instead, it might be an error or request for clarification.
      // But we can try to nudge it again if we haven't got a result.
      if (!result && outputSchema) {
         // Force it again? Or just break?
         // With Mode.ANY, it *should* call a tool. If it didn't, maybe it refused?
         // We can try sending a reminder.
         response = await chat.sendMessage(`Please submit the final answer using the ${outputToolName} tool.`);
      } else {
          break;
      }
    }
    turns++;
  }

  return result;
}
