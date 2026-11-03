package anthropic

import (
	"fmt"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// terminalTextItemAdded builds a text output-item start for reverse stream conversion.
func terminalTextItemAdded(itemID string, outputIndex int) *schemas.BifrostResponsesStreamResponse {
	return &schemas.BifrostResponsesStreamResponse{
		Type:        schemas.ResponsesStreamResponseTypeOutputItemAdded,
		OutputIndex: schemas.Ptr(outputIndex),
		Item: &schemas.ResponsesMessage{
			ID:   schemas.Ptr(itemID),
			Type: schemas.Ptr(schemas.ResponsesMessageTypeMessage),
		},
	}
}

// terminalTextDelta builds one normalized output-text delta.
func terminalTextDelta(itemID string, outputIndex int, text string) *schemas.BifrostResponsesStreamResponse {
	return &schemas.BifrostResponsesStreamResponse{
		Type:        schemas.ResponsesStreamResponseTypeOutputTextDelta,
		ItemID:      schemas.Ptr(itemID),
		OutputIndex: schemas.Ptr(outputIndex),
		Delta:       schemas.Ptr(text),
	}
}

// terminalTextDone builds the cumulative normalized output-text terminal event.
func terminalTextDone(itemID string, outputIndex int, text string) *schemas.BifrostResponsesStreamResponse {
	return &schemas.BifrostResponsesStreamResponse{
		Type:        schemas.ResponsesStreamResponseTypeOutputTextDone,
		ItemID:      schemas.Ptr(itemID),
		OutputIndex: schemas.Ptr(outputIndex),
		Text:        schemas.Ptr(text),
	}
}

// terminalTextItemDone builds the output-item terminal event that closes the Anthropic block.
func terminalTextItemDone(itemID string, outputIndex int) *schemas.BifrostResponsesStreamResponse {
	return &schemas.BifrostResponsesStreamResponse{
		Type:        schemas.ResponsesStreamResponseTypeOutputItemDone,
		OutputIndex: schemas.Ptr(outputIndex),
		Item: &schemas.ResponsesMessage{
			ID:   schemas.Ptr(itemID),
			Type: schemas.Ptr(schemas.ResponsesMessageTypeMessage),
		},
	}
}

// convertTerminalTextEvents runs normalized events through the Bifrost-to-Anthropic converter.
func convertTerminalTextEvents(ctx *schemas.BifrostContext, responses ...*schemas.BifrostResponsesStreamResponse) []*AnthropicStreamEvent {
	var events []*AnthropicStreamEvent
	for _, response := range responses {
		events = append(events, ToAnthropicResponsesStreamResponse(ctx, response)...)
	}
	return events
}

// terminalTextDeltas reconstructs client-visible text from Anthropic text deltas only.
func terminalTextDeltas(events []*AnthropicStreamEvent) string {
	var text string
	for _, event := range events {
		if event.Type == AnthropicStreamEventTypeContentBlockDelta &&
			event.Delta != nil && event.Delta.Type == AnthropicStreamDeltaTypeText && event.Delta.Text != nil {
			text += *event.Delta.Text
		}
	}
	return text
}

// TestTerminalTextStreamEmitsOnlyMissingSuffix verifies held and partially released output is completed once.
func TestTerminalTextStreamEmitsOnlyMissingSuffix(t *testing.T) {
	tests := []struct {
		name         string
		deltas       []string
		aggregate    string
		want         string
		wantTerminal string
	}{
		{name: "short held reply", deltas: []string{"", ""}, aggregate: "OK", want: "OK", wantTerminal: "OK"},
		{name: "partially released reply", deltas: []string{"First sentence. "}, aggregate: "First sentence. Final sentence.", want: "First sentence. Final sentence.", wantTerminal: "Final sentence."},
		{name: "normal stream", deltas: []string{"O", "K"}, aggregate: "OK", want: "OK", wantTerminal: ""},
		{name: "redaction changes byte length", deltas: []string{"Contact [EMAIL]. "}, aggregate: "Contact [EMAIL]. Done.", want: "Contact [EMAIL]. Done.", wantTerminal: "Done."},
		{name: "unicode prefix", deltas: []string{"Hello 世界. "}, aggregate: "Hello 世界. Done.", want: "Hello 世界. Done.", wantTerminal: "Done."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(nil, time.Time{})
			responses := []*schemas.BifrostResponsesStreamResponse{terminalTextItemAdded("msg_1", 0)}
			for _, delta := range test.deltas {
				responses = append(responses, terminalTextDelta("msg_1", 0, delta))
			}

			beforeTerminal := convertTerminalTextEvents(ctx, responses...)
			terminalEvents := convertTerminalTextEvents(ctx, terminalTextDone("msg_1", 0, test.aggregate))
			allEvents := append(beforeTerminal, terminalEvents...)
			if got := terminalTextDeltas(allEvents); got != test.want {
				t.Fatalf("reconstructed text = %q, want %q", got, test.want)
			}
			if got := terminalTextDeltas(terminalEvents); got != test.wantTerminal {
				t.Fatalf("terminal text delta = %q, want %q", got, test.wantTerminal)
			}
		})
	}
}

// TestTerminalTextStreamTracksBlocksIndependently verifies interleaved text items do not share progress.
func TestTerminalTextStreamTracksBlocksIndependently(t *testing.T) {
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	events := convertTerminalTextEvents(ctx,
		terminalTextItemAdded("msg_1", 0),
		terminalTextDelta("msg_1", 0, "First "),
		terminalTextItemAdded("msg_2", 1),
		terminalTextDelta("msg_2", 1, "Second "),
		terminalTextDone("msg_1", 0, "First done."),
		terminalTextDone("msg_2", 1, "Second done."),
	)

	wantByBlock := map[int]string{0: "First done.", 1: "Second done."}
	gotByBlock := make(map[int]string)
	for _, event := range events {
		if event.Type == AnthropicStreamEventTypeContentBlockDelta && event.Index != nil &&
			event.Delta != nil && event.Delta.Type == AnthropicStreamDeltaTypeText && event.Delta.Text != nil {
			gotByBlock[*event.Index] += *event.Delta.Text
		}
	}
	for index, want := range wantByBlock {
		if got := gotByBlock[index]; got != want {
			t.Errorf("block %d text = %q, want %q", index, got, want)
		}
	}
}

// TestTerminalTextStreamRejectsNonAppendOnlyAggregate verifies unsafe terminal aggregates never synthesize guessed text.
func TestTerminalTextStreamRejectsNonAppendOnlyAggregate(t *testing.T) {
	tests := []struct {
		name      string
		aggregate string
	}{
		{name: "shorter", aggregate: "Fir"},
		{name: "prefix mismatch", aggregate: "Changed sentence. Final."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(nil, time.Time{})
			convertTerminalTextEvents(ctx,
				terminalTextItemAdded("msg_1", 0),
				terminalTextDelta("msg_1", 0, "First sentence. "),
			)
			if events := convertTerminalTextEvents(ctx, terminalTextDone("msg_1", 0, test.aggregate)); len(events) != 0 {
				t.Fatalf("unsafe aggregate emitted events: %#v", events)
			}
		})
	}

	t.Run("unopened block", func(t *testing.T) {
		ctx := schemas.NewBifrostContext(nil, time.Time{})
		if events := convertTerminalTextEvents(ctx, terminalTextDone("msg_missing", 0, "OK")); len(events) != 0 {
			t.Fatalf("unopened block emitted events: %#v", events)
		}
	})
}

// TestTerminalTextStreamPassthroughNeverSynthesizes verifies raw Claude Code frames remain authoritative.
func TestTerminalTextStreamPassthroughNeverSynthesizes(t *testing.T) {
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	SetResponsesStreamPassthrough(ctx)
	convertTerminalTextEvents(ctx, terminalTextItemAdded("msg_1", 0))

	if events := convertTerminalTextEvents(ctx, terminalTextDone("msg_1", 0, "OK")); len(events) != 0 {
		t.Fatalf("passthrough output_text.done synthesized events: %#v", events)
	}
}

// TestTerminalTextStreamDeltaPrecedesStop verifies the recovered suffix is emitted before its block closes.
func TestTerminalTextStreamDeltaPrecedesStop(t *testing.T) {
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	convertTerminalTextEvents(ctx, terminalTextItemAdded("msg_1", 0))
	terminalEvents := convertTerminalTextEvents(ctx, terminalTextDone("msg_1", 0, "OK"))
	stopEvents := convertTerminalTextEvents(ctx, terminalTextItemDone("msg_1", 0))
	events := append(terminalEvents, stopEvents...)

	if len(events) != 2 || events[0].Type != AnthropicStreamEventTypeContentBlockDelta || events[1].Type != AnthropicStreamEventTypeContentBlockStop {
		t.Fatalf("terminal event order = %#v, want text delta then block stop", events)
	}
	if progress := getOrCreateAnthropicToResponsesStreamState(ctx).emittedDeltaByBlock; len(progress) != 0 {
		t.Fatalf("emitted text progress leaked after block stop: %#v", progress)
	}
}

// terminalToolItem builds a function_call (or mcp_call) output item for reverse stream conversion.
func terminalToolItem(itemType schemas.ResponsesMessageType, itemID, name, arguments string) *schemas.ResponsesMessage {
	return &schemas.ResponsesMessage{
		ID:   schemas.Ptr(itemID),
		Type: schemas.Ptr(itemType),
		ResponsesToolMessage: &schemas.ResponsesToolMessage{
			CallID:    schemas.Ptr(itemID),
			Name:      schemas.Ptr(name),
			Arguments: schemas.Ptr(arguments),
		},
	}
}

// terminalToolEvent builds one normalized tool-call stream event.
func terminalToolEvent(eventType schemas.ResponsesStreamResponseType, itemID string, outputIndex int) *schemas.BifrostResponsesStreamResponse {
	return &schemas.BifrostResponsesStreamResponse{
		Type:        eventType,
		ItemID:      schemas.Ptr(itemID),
		OutputIndex: schemas.Ptr(outputIndex),
	}
}

// terminalToolStream builds the normalized events for one tool call: output_item.added
// carrying addedArguments, the given arguments deltas, the cumulative *_arguments.done,
// and output_item.done.
func terminalToolStream(itemType schemas.ResponsesMessageType, itemID, name string, outputIndex int, addedArguments string, deltas []string, arguments string) []*schemas.BifrostResponsesStreamResponse {
	deltaType, doneType := schemas.ResponsesStreamResponseTypeFunctionCallArgumentsDelta, schemas.ResponsesStreamResponseTypeFunctionCallArgumentsDone
	if itemType == schemas.ResponsesMessageTypeMCPCall {
		deltaType, doneType = schemas.ResponsesStreamResponseTypeMCPCallArgumentsDelta, schemas.ResponsesStreamResponseTypeMCPCallArgumentsDone
	}
	responses := []*schemas.BifrostResponsesStreamResponse{{
		Type:        schemas.ResponsesStreamResponseTypeOutputItemAdded,
		OutputIndex: schemas.Ptr(outputIndex),
		Item:        terminalToolItem(itemType, itemID, name, addedArguments),
	}}
	for _, delta := range deltas {
		event := terminalToolEvent(deltaType, itemID, outputIndex)
		event.Delta = schemas.Ptr(delta)
		responses = append(responses, event)
	}
	done := terminalToolEvent(doneType, itemID, outputIndex)
	done.Arguments = schemas.Ptr(arguments)
	return append(responses, done, &schemas.BifrostResponsesStreamResponse{
		Type:        schemas.ResponsesStreamResponseTypeOutputItemDone,
		OutputIndex: schemas.Ptr(outputIndex),
		Item:        terminalToolItem(itemType, itemID, name, arguments),
	})
}

// terminalToolInputs reconstructs client-visible tool input per block from input_json deltas only.
func terminalToolInputs(events []*AnthropicStreamEvent) map[int]string {
	inputs := make(map[int]string)
	for _, event := range events {
		if event.Type == AnthropicStreamEventTypeContentBlockDelta && event.Index != nil &&
			event.Delta != nil && event.Delta.Type == AnthropicStreamDeltaTypeInputJSON && event.Delta.PartialJSON != nil {
			inputs[*event.Index] += *event.Delta.PartialJSON
		}
	}
	return inputs
}

// TestTerminalToolInputEmitsOnlyMissingSuffix verifies tool input carried only by the
// cumulative *_arguments.done (OpenAI streams parallel calls without arguments deltas)
// reaches the client exactly once.
func TestTerminalToolInputEmitsOnlyMissingSuffix(t *testing.T) {
	const arguments = `{"file_path":"/tmp/日本.txt"}`
	tests := []struct {
		name           string
		itemType       schemas.ResponsesMessageType
		addedArguments string
		deltas         []string
		want           string
	}{
		{name: "no deltas", itemType: schemas.ResponsesMessageTypeFunctionCall, want: arguments},
		{name: "partial deltas", itemType: schemas.ResponsesMessageTypeFunctionCall, deltas: []string{`{"file_path":`}, want: arguments},
		{name: "normal stream", itemType: schemas.ResponsesMessageTypeFunctionCall, deltas: []string{`{"file_path":`, `"/tmp/日本.txt"}`}, want: arguments},
		{name: "arguments on output_item.added", itemType: schemas.ResponsesMessageTypeFunctionCall, addedArguments: arguments, want: arguments},
		{name: "mcp call without deltas", itemType: schemas.ResponsesMessageTypeMCPCall, want: arguments},
		{name: "divergent aggregate", itemType: schemas.ResponsesMessageTypeFunctionCall, deltas: []string{`{"file_path": "/tmp/a"}`}, want: `{"file_path": "/tmp/a"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(nil, time.Time{})
			events := convertTerminalTextEvents(ctx, terminalToolStream(test.itemType, "fc_1", "Read", 0, test.addedArguments, test.deltas, arguments)...)
			if got := terminalToolInputs(events)[0]; got != test.want {
				t.Fatalf("reconstructed input = %q, want %q", got, test.want)
			}
		})
	}
}

// TestTerminalToolInputParallelCallsWithoutDeltas pins the reported shape: several calls
// in one turn, none with arguments deltas, each block keeping its own arguments.
func TestTerminalToolInputParallelCallsWithoutDeltas(t *testing.T) {
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	want := map[int]string{0: `{"url":"https://a.example"}`, 1: `{"url":"https://b.example"}`, 2: `{"query":"bifrost"}`}
	var responses []*schemas.BifrostResponsesStreamResponse
	for index, name := range []string{"WebFetch", "WebFetch", "WebSearch"} {
		responses = append(responses, terminalToolStream(schemas.ResponsesMessageTypeFunctionCall, fmt.Sprintf("fc_%d", index), name, index, "", nil, want[index])...)
	}

	got := terminalToolInputs(convertTerminalTextEvents(ctx, responses...))
	for index, input := range want {
		if got[index] != input {
			t.Errorf("block %d input = %q, want %q", index, got[index], input)
		}
	}
}

// TestTerminalToolInputPassthroughNeverSynthesizes verifies raw Claude Code frames remain authoritative.
func TestTerminalToolInputPassthroughNeverSynthesizes(t *testing.T) {
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	SetResponsesStreamPassthrough(ctx)
	responses := terminalToolStream(schemas.ResponsesMessageTypeFunctionCall, "toolu_1", "Read", 0, "", nil, `{"file_path":"/tmp/a"}`)
	convertTerminalTextEvents(ctx, responses[0])

	if events := convertTerminalTextEvents(ctx, responses[1]); len(events) != 0 {
		t.Fatalf("passthrough function_call_arguments.done synthesized events: %#v", events)
	}
}

// BenchmarkTerminalToolInputStream measures one streamed tool call through the Anthropic
// egress, with and without arguments deltas.
func BenchmarkTerminalToolInputStream(b *testing.B) {
	const arguments = `{"file_path":"/home/user/project/src/main.go","offset":120,"limit":400}`
	var deltas []string
	for i := 0; i < len(arguments); i += 8 {
		deltas = append(deltas, arguments[i:min(i+8, len(arguments))])
	}
	for _, bench := range []struct {
		name   string
		deltas []string
	}{{"deltas", deltas}, {"no deltas", nil}} {
		responses := terminalToolStream(schemas.ResponsesMessageTypeFunctionCall, "fc_1", "Read", 0, "", bench.deltas, arguments)
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ctx := schemas.NewBifrostContext(nil, time.Time{})
				for _, response := range responses {
					ToAnthropicResponsesStreamResponse(ctx, response)
				}
			}
		})
	}
}
