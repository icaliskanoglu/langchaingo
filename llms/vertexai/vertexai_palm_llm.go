package vertexai

import (
	"context"
	"errors"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/vertexai/internal/vertexaiclient"
	"github.com/tmc/langchaingo/schema"
)

var (
	ErrEmptyResponse            = errors.New("no response")
	ErrMissingProjectID         = errors.New("missing the GCP Project ID, set it in the GOOGLE_CLOUD_PROJECT environment variable") //nolint:lll
	ErrUnexpectedResponseLength = errors.New("unexpected length of response")
	ErrNotImplemented           = errors.New("not implemented")
)

const (
	userAuthor = "user"
	botAuthor  = "bot"
)

type LLM struct {
	client *vertexaiclient.PaLMClient
}

var (
	_ llms.LLM           = (*LLM)(nil)
	_ llms.LanguageModel = (*LLM)(nil)
)

// Call requests a completion for the given prompt.
func (o *LLM) Call(ctx context.Context, prompt string, options ...llms.CallOption) (string, error) {
	r, err := o.Generate(ctx, []string{prompt}, options...)
	if err != nil {
		return "", err
	}
	if len(r) == 0 {
		return "", ErrEmptyResponse
	}
	return r[0].Text, nil
}

func (o *LLM) Generate(ctx context.Context, prompts []string, options ...llms.CallOption) ([]*llms.Generation, error) {
	opts := llms.CallOptions{}
	for _, opt := range options {
		opt(&opts)
	}
	results, err := o.client.CreateCompletion(ctx, &vertexaiclient.CompletionRequest{
		Prompts:     prompts,
		MaxTokens:   opts.MaxTokens,
		Temperature: opts.Temperature,
	})
	if err != nil {
		return nil, err
	}

	generations := []*llms.Generation{}
	for _, r := range results {
		generations = append(generations, &llms.Generation{
			Text: r.Text,
		})
	}
	return generations, nil
}

// CreateEmbedding creates embeddings for the given input texts.
func (o *LLM) CreateEmbedding(ctx context.Context, inputTexts []string) ([][]float64, error) {
	embeddings, err := o.client.CreateEmbedding(ctx, &vertexaiclient.EmbeddingRequest{
		Input: inputTexts,
	})
	if err != nil {
		return [][]float64{}, err
	}

	if len(embeddings) == 0 {
		return [][]float64{}, ErrEmptyResponse
	}
	if len(inputTexts) != len(embeddings) {
		return embeddings, ErrUnexpectedResponseLength
	}

	return embeddings, nil
}

func (o *LLM) GeneratePrompt(ctx context.Context, promptValues []schema.PromptValue, options ...llms.CallOption) (llms.LLMResult, error) { //nolint:lll
	return llms.GeneratePrompt(ctx, o, promptValues, options...)
}

func (o *LLM) GetNumTokens(text string) int {
	return llms.CountTokens(vertexaiclient.TextModelName, text)
}

type ChatMessage = vertexaiclient.ChatMessage

type Chat struct {
	client *vertexaiclient.PaLMClient
}

var (
	_ llms.ChatLLM       = (*Chat)(nil)
	_ llms.LanguageModel = (*Chat)(nil)
)

// Chat requests a chat response for the given messages.
func (o *Chat) Call(ctx context.Context, messages []schema.ChatMessage, options ...llms.CallOption) (*schema.AIChatMessage, error) { // nolint: lll
	r, err := o.Generate(ctx, [][]schema.ChatMessage{messages}, options...)
	if err != nil {
		return nil, err
	}
	if len(r) == 0 {
		return nil, ErrEmptyResponse
	}
	return r[0].Message, nil
}

// Generate requests a chat response for each of the sets of messages.
func (o *Chat) Generate(ctx context.Context, messageSets [][]schema.ChatMessage, options ...llms.CallOption) ([]*llms.Generation, error) { // nolint: lll
	opts := llms.CallOptions{}
	for _, opt := range options {
		opt(&opts)
	}
	if opts.StreamingFunc != nil {
		return nil, ErrNotImplemented
	}

	generations := make([]*llms.Generation, 0, len(messageSets))
	for _, messages := range messageSets {
		msgs := toClientChatMessage(messages)
		result, err := o.client.CreateChat(ctx, &vertexaiclient.ChatRequest{
			Temperature: opts.Temperature,
			Messages:    msgs,
		})
		if err != nil {
			return nil, err
		}
		if len(result.Candidates) == 0 {
			return nil, ErrEmptyResponse
		}
		generations = append(generations, &llms.Generation{
			Message: &schema.AIChatMessage{
				Content: result.Candidates[0].Content,
			},
			Text: result.Candidates[0].Content,
		})
	}

	return generations, nil
}

func (o *Chat) GeneratePrompt(ctx context.Context, promptValues []schema.PromptValue, options ...llms.CallOption) (llms.LLMResult, error) { //nolint:lll
	return llms.GenerateChatPrompt(ctx, o, promptValues, options...)
}

func (o *Chat) GetNumTokens(text string) int {
	return llms.CountTokens(vertexaiclient.TextModelName, text)
}

func toClientChatMessage(messages []schema.ChatMessage) []*vertexaiclient.ChatMessage {
	msgs := make([]*vertexaiclient.ChatMessage, len(messages))
	for i, m := range messages {
		msg := &vertexaiclient.ChatMessage{
			Content: m.GetContent(),
		}
		typ := m.GetType()
		switch typ {
		case schema.ChatMessageTypeSystem:
			msg.Author = botAuthor
		case schema.ChatMessageTypeAI:
			msg.Author = botAuthor
		case schema.ChatMessageTypeHuman:
			msg.Author = userAuthor
		case schema.ChatMessageTypeGeneric:
			msg.Author = userAuthor
		case schema.ChatMessageTypeFunction:
			msg.Author = userAuthor
		}
		if n, ok := m.(schema.Named); ok {
			msg.Author = n.GetName()
		}
		msgs[i] = msg
	}
	return msgs
}

// New returns a new VertexAI PaLM LLM.
func New(opts ...Option) (*LLM, error) {
	client, err := newClient(opts...)
	return &LLM{client: client}, err
}

// New returns a new VertexAI PaLM Chat LLM.
func NewChat(opts ...Option) (*Chat, error) {
	client, err := newClient(opts...)
	return &Chat{client: client}, err
}

func newClient(opts ...Option) (*vertexaiclient.PaLMClient, error) {
	// Ensure options are initialized only once.
	initOptions.Do(initOpts)
	options := &options{}
	*options = *defaultOptions // Copy default options.

	for _, opt := range opts {
		opt(options)
	}
	if len(options.projectID) == 0 {
		return nil, ErrMissingProjectID
	}

	return vertexaiclient.New(options.projectID, options.clientOptions...)
}
