package pangolin

import (
	"fmt"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/xuri/excelize/v2"
)

type Pangolin struct {
	df dataframe.DataFrame
}

func New() *Pangolin {
	return &Pangolin{}
}

// Métodos para acessar funcionalidades do gota
func (p *Pangolin) DataFrame() dataframe.DataFrame {
	return p.df
}

func (p *Pangolin) SetDataFrame(df dataframe.DataFrame) *Pangolin {
	p.df = df
	return p
}

// Wrapper methods para funcionalidades comuns do gota
func (p *Pangolin) Dims() (int, int) {
	return p.df.Dims()
}

func (p *Pangolin) Nrows() int {
	nrows, _ := p.df.Dims()
	return nrows
}

func (p *Pangolin) Ncols() int {
	_, ncols := p.df.Dims()
	return ncols
}

func (p *Pangolin) Names() []string {
	return p.df.Names()
}

func (p *Pangolin) Types() []series.Type {
	return p.df.Types()
}

func (p *Pangolin) String() string {
	return p.df.String()
}

func (p *Pangolin) Head(n int) *Pangolin {
	nrows, _ := p.df.Dims()
	indices := make([]int, n)
	for i := range n {
		indices[i] = i
	}
	return &Pangolin{df: p.df.Subset(indices[:min(n, nrows)])}
}

func (p *Pangolin) Tail(n int) *Pangolin {
	nrows, _ := p.df.Dims()
	start := max(0, nrows-n)
	indices := make([]int, 0, n)
	for i := start; i < nrows; i++ {
		indices = append(indices, i)
	}
	return &Pangolin{df: p.df.Subset(indices)}
}

func (p *Pangolin) Select(columns ...string) *Pangolin {
	return &Pangolin{df: p.df.Select(columns)}
}

func (p *Pangolin) Filter(filters ...dataframe.F) *Pangolin {
	return &Pangolin{df: p.df.Filter(filters...)}
}

//TODO: Implementar a abstração para mutate
// func (p *Pangolin) Mutate(mutators ...series.Series) *Pangolin {
// 	return &Pangolin{df: p.df.Mutate(mutators...)}
// }

func (p *Pangolin) Arrange(keys ...dataframe.Order) *Pangolin {
	return &Pangolin{df: p.df.Arrange(keys...)}
}

//TODO: Implementar a abstração para group_by
// func (p *Pangolin) GroupBy(columns ...string) *Pangolin {
// 	return &Pangolin{df: p.df.GroupBy(columns...)}
// }

//TODO: Implementar a abstração para aggregation
// func (p *Pangolin) Aggregation(aggs []dataframe.AggregationType, colnames []string) *Pangolin {
// 	return &Pangolin{df: p.df.Aggregation(aggs, colnames)}
// }

func (p *Pangolin) InnerJoin(other *Pangolin, keys ...string) *Pangolin {
	return &Pangolin{df: p.df.InnerJoin(other.df, keys...)}
}

func (p *Pangolin) LeftJoin(other *Pangolin, keys ...string) *Pangolin {
	return &Pangolin{df: p.df.LeftJoin(other.df, keys...)}
}

func (p *Pangolin) RightJoin(other *Pangolin, keys ...string) *Pangolin {
	return &Pangolin{df: p.df.RightJoin(other.df, keys...)}
}

func (p *Pangolin) OuterJoin(other *Pangolin, keys ...string) *Pangolin {
	return &Pangolin{df: p.df.OuterJoin(other.df, keys...)}
}

func (p *Pangolin) CrossJoin(other *Pangolin) *Pangolin {
	return &Pangolin{df: p.df.CrossJoin(other.df)}
}

func (p *Pangolin) Concat(other *Pangolin) *Pangolin {
	return &Pangolin{df: p.df.Concat(other.df)}
}

func (p *Pangolin) CBind(other *Pangolin) *Pangolin {
	return &Pangolin{df: p.df.CBind(other.df)}
}

func (p *Pangolin) RBind(other *Pangolin) *Pangolin {
	return &Pangolin{df: p.df.RBind(other.df)}
}

func (p *Pangolin) Describe() *Pangolin {
	return &Pangolin{df: p.df.Describe()}
}

// Métodos para acessar colunas específicas
func (p *Pangolin) Col(colname string) series.Series {
	return p.df.Col(colname)
}

func (p *Pangolin) Elem(row, col int) series.Element {
	return p.df.Elem(row, col)
}

// Métodos para conversão
func (p *Pangolin) Records() [][]string {
	return p.df.Records()
}

func (p *Pangolin) Maps() []map[string]interface{} {
	return p.df.Maps()
}

func (p *Pangolin) ReadExcel(path, sheetName string, hasHeader bool) (*Pangolin, error) {
	var (
		headerSize int
		sb         strings.Builder
		rows       [][]string
	)

	if path == "" {
		return p, fmt.Errorf("filename is required")
	}

	f, err := excelize.OpenFile(path)
	if err != nil {
		return p, err
	}
	defer f.Close()

	if sheetName != "" {
		rows, err = f.GetRows(sheetName)
		if err != nil {
			return p, err
		}
	} else if sheetName == "" {
		sheets := f.GetSheetList()
		rows, err = f.GetRows(sheets[0])
		if err != nil {
			return p, err
		}
	}
	if len(rows) == 0 {
		return p, fmt.Errorf("no data found in the file")
	}

	if hasHeader {
		header := rows[0]
		headerSize = len(header)
		sb.WriteString(strings.Join(header, ";") + "\n")
	} else {
		header := make([]string, maxCols(rows))
		for i := range header {
			header[i] = fmt.Sprintf("col%d", i+1)
		}
		headerSize = len(header)
		sb.WriteString(strings.Join(header, ";") + "\n")
	}

	for _, row := range rows {
		normalized := make([]string, headerSize)
		for i := range headerSize {
			if i < len(row) {
				cell := strings.ReplaceAll(row[i], "\n", " ")
				cell = strings.TrimSpace(cell)
				normalized[i] = cell
			} else {
				normalized[i] = ""
			}
		}
		sb.WriteString(strings.Join(normalized, ";") + "\n")
	}

	fmt.Println(sb.String())
	p.df = dataframe.ReadCSV(strings.NewReader(sb.String()),
		dataframe.WithDelimiter(';'),
		dataframe.DetectTypes(false),
	)
	fmt.Println(p.df.String())
	return p, nil
}

// Método para criar DataFrame a partir de dados
func (p *Pangolin) FromRecords(records [][]string) *Pangolin {
	sb := strings.Builder{}
	for i, row := range records {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(strings.Join(row, ";"))
	}

	p.df = dataframe.ReadCSV(strings.NewReader(sb.String()))
	return p
}

// Método para criar DataFrame a partir de maps
func (p *Pangolin) FromMaps(maps []map[string]any) *Pangolin {
	df := dataframe.LoadMaps(maps)
	p.df = df
	return p
}

// Método para criar DataFrame a partir de series
func (p *Pangolin) FromSeries(series ...series.Series) *Pangolin {
	df := dataframe.New(series...)
	p.df = df
	return p
}

// Funções auxiliares
func maxCols(rows [][]string) int {
	max := 0
	for _, row := range rows {
		if len(row) > max {
			max = len(row)
		}
	}
	return max
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}


