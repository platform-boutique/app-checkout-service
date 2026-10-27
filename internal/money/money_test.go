package money

import (
	"testing"

	pb "checkout-service/proto"
)

// --- helpers ---

func usd(units int64, nanos int32) *pb.Money {
	return &pb.Money{CurrencyCode: "USD", Units: units, Nanos: nanos}
}

func eur(units int64, nanos int32) *pb.Money {
	return &pb.Money{CurrencyCode: "EUR", Units: units, Nanos: nanos}
}

// ─── IsValid ──────────────────────────────────────────────────────────────────

func TestIsValid(t *testing.T) {
	tests := []struct {
		name  string
		money *pb.Money
		want  bool
	}{
		{"zero value", usd(0, 0), true},
		{"positive units only", usd(5, 0), true},
		{"positive nanos only", usd(0, 500000000), true},
		{"positive units and nanos", usd(5, 500000000), true},
		{"negative units only", usd(-5, 0), true},
		{"negative nanos only", usd(0, -500000000), true},
		{"negative units and nanos", usd(-5, -500000000), true},
		{"nanos at max boundary", usd(0, 999999999), true},
		{"nanos at min boundary", usd(0, -999999999), true},
		{"sign mismatch: positive units negative nanos", usd(5, -1), false},
		{"sign mismatch: negative units positive nanos", usd(-5, 1), false},
		{"nanos exceeds max", usd(0, 1000000000), false},
		{"nanos exceeds min", usd(0, -1000000000), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValid(tt.money); got != tt.want {
				t.Errorf("IsValid(%+v) = %v, want %v", tt.money, got, tt.want)
			}
		})
	}
}

// ─── IsZero ───────────────────────────────────────────────────────────────────

func TestIsZero(t *testing.T) {
	tests := []struct {
		name  string
		money *pb.Money
		want  bool
	}{
		{"zero", usd(0, 0), true},
		{"non-zero units", usd(1, 0), false},
		{"non-zero nanos", usd(0, 1), false},
		{"both non-zero", usd(1, 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsZero(tt.money); got != tt.want {
				t.Errorf("IsZero(%+v) = %v, want %v", tt.money, got, tt.want)
			}
		})
	}
}

// ─── IsPositive ───────────────────────────────────────────────────────────────

func TestIsPositive(t *testing.T) {
	tests := []struct {
		name  string
		money *pb.Money
		want  bool
	}{
		{"positive units", usd(1, 0), true},
		{"positive nanos only", usd(0, 1), true},
		{"zero", usd(0, 0), false},
		{"negative units", usd(-1, 0), false},
		{"negative nanos", usd(0, -1), false},
		{"invalid sign mismatch", usd(5, -1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPositive(tt.money); got != tt.want {
				t.Errorf("IsPositive(%+v) = %v, want %v", tt.money, got, tt.want)
			}
		})
	}
}

// ─── IsNegative ───────────────────────────────────────────────────────────────

func TestIsNegative(t *testing.T) {
	tests := []struct {
		name  string
		money *pb.Money
		want  bool
	}{
		{"negative units", usd(-1, 0), true},
		{"negative nanos only", usd(0, -1), true},
		{"zero", usd(0, 0), false},
		{"positive units", usd(1, 0), false},
		{"positive nanos", usd(0, 1), false},
		{"invalid sign mismatch", usd(-5, 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNegative(tt.money); got != tt.want {
				t.Errorf("IsNegative(%+v) = %v, want %v", tt.money, got, tt.want)
			}
		})
	}
}

// ─── AreSameCurrency ─────────────────────────────────────────────────────────

func TestAreSameCurrency(t *testing.T) {
	tests := []struct {
		name string
		l, r *pb.Money
		want bool
	}{
		{"same currency", usd(1, 0), usd(2, 0), true},
		{"different currency", usd(1, 0), eur(1, 0), false},
		{"both empty currency", &pb.Money{Units: 1}, &pb.Money{Units: 2}, false},
		{"one empty currency", usd(1, 0), &pb.Money{Units: 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AreSameCurrency(tt.l, tt.r); got != tt.want {
				t.Errorf("AreSameCurrency(%+v, %+v) = %v, want %v", tt.l, tt.r, got, tt.want)
			}
		})
	}
}

// ─── AreEquals ───────────────────────────────────────────────────────────────

func TestAreEquals(t *testing.T) {
	tests := []struct {
		name string
		l, r *pb.Money
		want bool
	}{
		{"identical", usd(5, 500000000), usd(5, 500000000), true},
		{"different units", usd(5, 0), usd(6, 0), false},
		{"different nanos", usd(5, 100), usd(5, 200), false},
		{"different currency", usd(5, 0), eur(5, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AreEquals(tt.l, tt.r); got != tt.want {
				t.Errorf("AreEquals(%+v, %+v) = %v, want %v", tt.l, tt.r, got, tt.want)
			}
		})
	}
}

// ─── Negate ──────────────────────────────────────────────────────────────────

func TestNegate(t *testing.T) {
	tests := []struct {
		name  string
		input *pb.Money
		want  *pb.Money
	}{
		{"positive", usd(5, 500000000), usd(-5, -500000000)},
		{"negative", usd(-3, -200000000), usd(3, 200000000)},
		{"zero", usd(0, 0), usd(0, 0)},
		{"preserves currency", eur(1, 0), eur(-1, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Negate(tt.input)
			if !AreEquals(&got, tt.want) {
				t.Errorf("Negate(%+v) = %+v, want %+v", tt.input, &got, tt.want)
			}
		})
	}
}

// ─── Sum ─────────────────────────────────────────────────────────────────────

func TestSum(t *testing.T) {
	tests := []struct {
		name    string
		l, r    *pb.Money
		want    *pb.Money
		wantErr error
	}{
		{
			name: "simple addition",
			l:    usd(1, 0),
			r:    usd(2, 0),
			want: usd(3, 0),
		},
		{
			name: "nanos carry over to units",
			l:    usd(1, 900000000),
			r:    usd(0, 200000000),
			want: usd(2, 100000000),
		},
		{
			name: "adding zero",
			l:    usd(5, 500000000),
			r:    usd(0, 0),
			want: usd(5, 500000000),
		},
		{
			name: "negative values",
			l:    usd(-1, -500000000),
			r:    usd(-2, -300000000),
			want: usd(-3, -800000000),
		},
		{
			name: "positive + negative cancels out",
			l:    usd(5, 0),
			r:    usd(-5, 0),
			want: usd(0, 0),
		},
		{
			name: "both below one unit, no carry (0.1 + 0.2)",
			l:    usd(0, 100000000),
			r:    usd(0, 200000000),
			want: usd(0, 300000000),
		},
		{
			name: "both below one unit, carry (0.6 + 0.7)",
			l:    usd(0, 600000000),
			r:    usd(0, 700000000),
			want: usd(1, 300000000),
		},
		{
			name: "both negative below one unit (-0.1 + -0.2)",
			l:    usd(0, -100000000),
			r:    usd(0, -200000000),
			want: usd(0, -300000000),
		},
		{
			name: "cross-sign: positive units negative nanos carry",
			l:    usd(2, 0),
			r:    usd(0, -500000000),
			want: usd(1, 500000000),
		},
		{
			name: "cross-sign: negative units positive nanos carry",
			l:    usd(-2, 0),
			r:    usd(0, 500000000),
			want: usd(-1, -500000000),
		},
		{
			name:    "mismatching currency",
			l:       usd(1, 0),
			r:       eur(1, 0),
			wantErr: ErrMismatchingCurrency,
		},
		{
			name:    "invalid left operand",
			l:       usd(5, -1),
			r:       usd(1, 0),
			wantErr: ErrInvalidValue,
		},
		{
			name:    "invalid right operand",
			l:       usd(1, 0),
			r:       usd(-5, 1),
			wantErr: ErrInvalidValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Sum(tt.l, tt.r)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Sum(%+v, %+v) error = %v, want %v", tt.l, tt.r, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Sum(%+v, %+v) unexpected error: %v", tt.l, tt.r, err)
			}
			if !AreEquals(got, tt.want) {
				t.Errorf("Sum(%+v, %+v) = %+v, want %+v", tt.l, tt.r, got, tt.want)
			}
		})
	}
}

// ─── MultiplySlow ────────────────────────────────────────────────────────────

func TestMultiplySlow(t *testing.T) {
	tests := []struct {
		name  string
		money *pb.Money
		n     uint32
		want  *pb.Money
	}{
		{"multiply by 1 is identity", usd(5, 0), 1, usd(5, 0)},
		{"multiply by 2", usd(3, 500000000), 2, usd(7, 0)},
		{"multiply by 3", usd(1, 0), 3, usd(3, 0)},
		{"nanos carry on multiply", usd(0, 500000000), 3, usd(1, 500000000)},
		{"0.1 x 3 is exactly 0.3", usd(0, 100000000), 3, usd(0, 300000000)},
		{"33.333333333 x 3", usd(33, 333333333), 3, usd(99, 999999999)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MultiplySlow(tt.money, tt.n)
			if !AreEquals(got, tt.want) {
				t.Errorf("MultiplySlow(%+v, %d) = %+v, want %+v", tt.money, tt.n, got, tt.want)
			}
		})
	}
}

// ─── Must ────────────────────────────────────────────────────────────────────

func TestMust(t *testing.T) {
	t.Run("returns value on no error", func(t *testing.T) {
		got := Must(usd(5, 0), nil)
		if !AreEquals(got, usd(5, 0)) {
			t.Errorf("Must returned unexpected value: %+v", got)
		}
	})

	t.Run("panics on error", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Must did not panic on error")
			}
		}()
		Must(nil, ErrInvalidValue)
	})
}
